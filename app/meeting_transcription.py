from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openai import OpenAI


WHISPER_MODEL = os.getenv("OPENAI_WHISPER_MODEL", "whisper-1").strip() or "whisper-1"


def transcribe_audio_file(client: "OpenAI", audio_path: Path) -> str:
    with audio_path.open("rb") as fh:
        result = client.audio.transcriptions.create(model=WHISPER_MODEL, file=fh)
    text = getattr(result, "text", None) or ""
    return str(text).strip()


def summarize_meeting_transcript(client: "OpenAI", chat_model: str, meeting_name: str, transcript: str) -> str:
    if not (transcript or "").strip():
        return "No speech was detected in this recording."
    system = (
        "You summarize meeting transcripts for a product/engineering team.\n"
        "Output plain text (no markdown fences). Use short sections:\n"
        "Overview: one short paragraph.\n"
        "Key points: bullet list.\n"
        "Action items: bullet list with owner if mentioned.\n"
        "Keep the total under about 400 words unless the meeting was very long."
    )
    user = f"Meeting title: {meeting_name}\n\nTranscript:\n{transcript}"
    completion = client.chat.completions.create(
        model=chat_model,
        temperature=0.2,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
    )
    content = completion.choices[0].message.content or ""
    return str(content).strip()


def transcribe_and_summarize(
    client: "OpenAI",
    chat_model: str,
    audio_path: Path,
    meeting_name: str,
) -> tuple[str, str]:
    transcript = transcribe_audio_file(client, audio_path)
    summary = summarize_meeting_transcript(client, chat_model, meeting_name, transcript)
    return transcript, summary
