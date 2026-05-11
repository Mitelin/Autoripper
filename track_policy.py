from __future__ import annotations

import re
from typing import Any


LANGUAGE_ALIASES = {
    "cze": {"cze", "ces", "cz", "czech", "cestina", "cesky", "cz dabing", "czech dub"},
    "slo": {"slo", "slk", "sk", "slovak", "slovencina", "sk dabing", "slovak dub"},
    "eng": {"eng", "en", "english", "anglicky", "anglictina", "english dub", "en dub"},
    "jpn": {"jpn", "ja", "jp", "japanese", "japonstina", "nihongo", "original japanese", "japanese original"},
}
UNKNOWN_LANGUAGE_VALUES = {"", "und", "unknown", "none"}
COMMENTARY_TERMS = ("commentary", "commentary track", "director commentary", "audio commentary", "komentar", "komentář")


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().casefold()
    replacements = {
        "čeština": "cestina",
        "česky": "cesky",
        "angličtina": "anglictina",
        "slovenčina": "slovencina",
        "japonština": "japonstina",
        "komentář": "komentar",
    }
    for source, replacement in replacements.items():
        text = text.replace(source, replacement)
    text = re.sub(r"[._\-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_language(value: Any) -> str:
    text = normalize_text(value)
    if text in UNKNOWN_LANGUAGE_VALUES:
        return "und"
    for language, aliases in LANGUAGE_ALIASES.items():
        if text in aliases:
            return language
    return text or "und"


def detect_language(stream: dict[str, Any]) -> tuple[str, str, str]:
    raw_language = stream.get("language")
    title = stream.get("title")
    normalized = normalize_language(raw_language)
    if normalized != "und":
        return normalized, "high", "language tag detected"

    normalized_title = normalize_text(title)
    if normalized_title:
        for language, aliases in LANGUAGE_ALIASES.items():
            for alias in aliases:
                alias_text = normalize_text(alias)
                if re.search(rf"(^|\b){re.escape(alias_text)}(\b|$)", normalized_title):
                    return language, "high", "title clearly identifies language"
    return "und", "low", "language is unknown"


def is_commentary(stream: dict[str, Any]) -> bool:
    title = normalize_text(stream.get("title"))
    return any(term in title for term in COMMENTARY_TERMS)


def media_policy(config: dict[str, Any], media_type: str) -> dict[str, Any]:
    policy = config.get("track_policy") or {}
    return policy.get(media_type) or policy.get("unknown") or {}


def disabled_result(config: dict[str, Any], media_type: str, item: dict[str, Any], reason: str, confidence: str = "low") -> dict[str, Any]:
    return {
        "enabled": bool((config.get("track_policy") or {}).get("enabled", False)),
        "applied": False,
        "confidence": confidence,
        "media_type": media_type,
        "target_audio_languages": media_policy(config, media_type).get("target_audio_languages") or [],
        "fallback_used": True,
        "decision_summary": reason,
        "ffmpeg_mapping": "map_all",
        "map_arguments": ["-map", "0"],
        "selected_stream_indexes": None,
        "expected_audio_stream_count": item.get("audio_stream_count"),
        "expected_subtitle_stream_count": item.get("subtitle_stream_count"),
        "streams": build_keep_all_stream_decisions(item, reason),
    }


def build_keep_all_stream_decisions(item: dict[str, Any], reason: str) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    video_index = item.get("video_stream_index")
    if video_index is not None:
        decisions.append({"stream_index": video_index, "type": "video", "decision": "keep", "reason": reason})
    for stream in item.get("audio_streams") or []:
        language, confidence, language_reason = detect_language(stream)
        decisions.append(
            {
                "stream_index": stream.get("index"),
                "type": "audio",
                "codec": stream.get("codec"),
                "language_raw": stream.get("language"),
                "language_normalized": language,
                "title": stream.get("title"),
                "decision": "keep",
                "reason": reason,
                "confidence": confidence,
                "language_reason": language_reason,
            }
        )
    for stream in item.get("subtitle_streams") or []:
        language, confidence, language_reason = detect_language(stream)
        decisions.append(
            {
                "stream_index": stream.get("index"),
                "type": "subtitle",
                "codec": stream.get("codec"),
                "language_raw": stream.get("language"),
                "language_normalized": language,
                "title": stream.get("title"),
                "decision": "keep",
                "reason": "subtitle cleanup is conservative in this version",
                "confidence": confidence,
                "language_reason": language_reason,
            }
        )
    return decisions


def apply_track_policy(config: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    global_policy = config.get("track_policy") or {}
    media_type = item.get("media_type") or "unknown"
    if not global_policy.get("enabled", False):
        return disabled_result(config, media_type, item, "track policy disabled")

    per_media = media_policy(config, media_type)
    if per_media.get("cleanup_enabled", True) is False or media_type == "unknown":
        return disabled_result(config, media_type, item, "track cleanup disabled for media type")

    audio_streams = item.get("audio_streams") or []
    if len(audio_streams) <= 1:
        return disabled_result(config, media_type, item, "only one audio stream; preserving all streams", "high")

    target_languages = set(per_media.get("target_audio_languages") or [])
    if not target_languages:
        return disabled_result(config, media_type, item, "no target audio languages configured")

    audio_decisions: list[dict[str, Any]] = []
    target_audio_indexes: list[int] = []
    unknown_audio_found = False
    for stream in audio_streams:
        language, confidence, language_reason = detect_language(stream)
        stream_index = stream.get("index")
        commentary = is_commentary(stream)
        decision = "keep"
        reason = "kept until target audio confidence is established"
        if confidence == "high" and language in target_languages:
            target_audio_indexes.append(stream_index)
            reason = "target audio language detected with high confidence"
        elif language == "und":
            unknown_audio_found = True
            reason = "unknown language audio is not safe to drop"
        elif commentary:
            reason = "commentary track may be dropped only after target confidence is high"
        audio_decisions.append(
            {
                "stream_index": stream_index,
                "type": "audio",
                "codec": stream.get("codec"),
                "language_raw": stream.get("language"),
                "language_normalized": language,
                "title": stream.get("title"),
                "decision": decision,
                "reason": reason,
                "confidence": confidence,
                "language_reason": language_reason,
                "commentary": commentary,
            }
        )

    if not target_audio_indexes:
        return disabled_result(config, media_type, item, "target audio was not detected with high confidence")
    if unknown_audio_found:
        return disabled_result(config, media_type, item, "unknown-language audio exists; preserving all streams")
    if len(target_audio_indexes) >= len(audio_streams):
        return disabled_result(config, media_type, item, "all audio streams already match the target language", "high")

    selected_indexes: list[int] = []
    video_index = item.get("video_stream_index")
    if video_index is not None:
        selected_indexes.append(video_index)
    final_decisions: list[dict[str, Any]] = []
    if video_index is not None:
        final_decisions.append({"stream_index": video_index, "type": "video", "decision": "keep", "reason": "primary video stream"})

    target_audio_index_set = set(target_audio_indexes)
    for decision in audio_decisions:
        if decision["stream_index"] in target_audio_index_set:
            decision["decision"] = "keep"
            decision["reason"] = "target audio language detected with high confidence"
            selected_indexes.append(decision["stream_index"])
        elif decision.get("commentary"):
            decision["decision"] = "drop"
            decision["reason"] = "commentary track and target audio exists with high confidence"
        else:
            decision["decision"] = "drop"
            decision["reason"] = "non-target language and target audio exists with high confidence"
        final_decisions.append(decision)

    for stream in item.get("subtitle_streams") or []:
        stream_index = stream.get("index")
        if stream_index is not None:
            selected_indexes.append(stream_index)
        language, confidence, language_reason = detect_language(stream)
        final_decisions.append(
            {
                "stream_index": stream_index,
                "type": "subtitle",
                "codec": stream.get("codec"),
                "language_raw": stream.get("language"),
                "language_normalized": language,
                "title": stream.get("title"),
                "decision": "keep",
                "reason": "subtitle cleanup is conservative in this version",
                "confidence": confidence,
                "language_reason": language_reason,
            }
        )

    map_arguments: list[str] = []
    for stream_index in selected_indexes:
        map_arguments.extend(["-map", f"0:{stream_index}"])
    map_arguments.extend(["-map", "0:t?"])

    return {
        "enabled": True,
        "applied": True,
        "confidence": "high",
        "media_type": media_type,
        "target_audio_languages": sorted(target_languages),
        "fallback_used": False,
        "decision_summary": "Target audio detected with high confidence. Non-target audio streams were not mapped.",
        "ffmpeg_mapping": "explicit",
        "map_arguments": map_arguments,
        "selected_stream_indexes": selected_indexes,
        "expected_audio_stream_count": len(target_audio_indexes),
        "expected_subtitle_stream_count": item.get("subtitle_stream_count"),
        "streams": final_decisions,
    }
