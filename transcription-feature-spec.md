# Transcription Tab — Feature Spec

## What I wanted

A new **Transcribe** tab in the Sprint Engine app (alongside Dashboard and Sprint Report) that:

1. **Listens to a meeting in real time** via the browser microphone
2. **Shows the transcribed text live** as I speak — not just after I stop
3. **Summarizes the transcript on demand** with a single button click

---

## Use case

During sprint meetings, I want to open this tab, hit record, and have the tool continuously convert speech to text so I can see what was said without taking manual notes. When the meeting ends, I click Summarize and get a bullet-point summary of decisions, action items, and key topics — ready to paste into the sprint board or share with the team.

---

## How it was designed to work

- A **🎙 Start Recording** button captures audio via the browser's `MediaRecorder` API
- Audio is split into **4-second chunks** and sent to the backend (`POST /api/transcribe`)
- The backend calls **OpenAI Whisper** (`whisper-1`) on each chunk and returns the transcribed text
- Each returned segment is **appended live** to the transcript area — so text grows as you talk
- A **Summarize** button sends the full accumulated transcript to the LLM and returns a structured markdown summary
- A **Clear** button resets everything for the next meeting

---

## What was built

| Layer | What was added |
|---|---|
| Backend | `POST /api/transcribe` — receives audio blob, calls Whisper, returns `{text}` |
| Backend | `POST /api/transcribe/summarize` — receives transcript text, returns `{summary}` |
| Frontend | New `🎙` activity bar icon + `#transcribeView` tab |
| Frontend | Live transcript display, recording indicator, Summarize + Clear buttons |

---

## Why it was reverted

The mic button in the UI was unresponsive. Root cause was not fully diagnosed before the revert — likely a browser security context issue (`http://` vs `https://`), a `MediaRecorder` API compatibility issue, or a missing browser permission that silently blocked the mic access without visible feedback to the user.

---

## What needs to be resolved before re-implementing

- [ ] Confirm browser grants mic permission for `http://127.0.0.1:8001` (Chrome treats this as a secure context; Safari may not)
- [ ] Add an explicit check: if `navigator.mediaDevices` is `undefined`, show a clear error before trying to record
- [ ] Test `MediaRecorder` availability in the target browser before wiring up the button
- [ ] Consider running the app on `https://` locally (via a self-signed cert or `mkcert`) if mic permission remains blocked
