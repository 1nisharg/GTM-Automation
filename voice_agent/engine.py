"""
voice_agent/engine.py
----------------------
Bidirectional outbound voice agent.

Stack (all free / open-source friendly, cost-optimised for cloud):
  Twilio          — PSTN call + bidirectional media stream (mulaw 8 kHz)
  Deepgram        — live STT (Nova-2, streaming WebSocket)
  Groq            — LLM reply generation (llama-3.3-70b, streaming)
  edge-tts        — neural TTS (Microsoft Edge voices, free, no API key)
  miniaudio       — MP3→PCM decode + resample (pure C, no ffmpeg)
  audioop/struct  — PCM16→mulaw encode (stdlib, zero cost)
  PostgreSQL      — transcript + summary storage (shared pool)

Conversation loop (per call, inside audio_stream WebSocket):
  1. Twilio streams mulaw audio chunks → we forward to Deepgram
  2. Deepgram returns interim (partial) AND final STT results
  3. Interim results during agent speech = barge-in signal → cut TTS immediately
  4. Each FINAL utterance → appended to conversation history → Groq (streaming)
  5. Groq text chunks accumulated → edge-tts synthesises reply
  6. Reply audio (mulaw, 8 kHz) → base64 → Twilio `media` WS message, chunked
     so we can abort mid-stream the instant a barge-in is detected
  7. Loop until Twilio sends `stop` event or caller hangs up
  8. Post-call: Groq summarises full transcript → stored in DB
  9. asyncio.Event signals outreach_node that call is complete

Cost notes:
  - edge-tts: $0 (Microsoft Edge neural TTS, no account needed)
  - Groq llama-3.3-70b: ~$0.59/M input tokens, ~$0.79/M output tokens
  - Deepgram Nova-2: ~$0.0059/min
  - miniaudio: $0 (pip package, pure C binding)
  - No GPU, no local model, no ffmpeg required

Routes mounted onto main FastAPI app (backend/main.py):
  GET/POST  /twilio/outbound       — TwiML: opening say + stream connect
  POST      /twilio/status         — call lifecycle events from Twilio
  WS        /ws/audio/{call_sid}   — bidirectional audio bridge

Public API for outreach_node:
  result = await place_call(to, script, mission, partner_id, partner_name,
                             contact_name=..., digitisation=..., category=...,
                             company_synopsis=...)
  category and company_synopsis drive personalized opening/pitch phrasing;
  both are optional and degrade gracefully to generic phrasing when absent.
"""

import asyncio
import base64
import json
import logging
import os
import re
import struct
import math
import time
import wave
from datetime import datetime, timezone
from datetime import timedelta
from pathlib import Path
from typing import Optional

import edge_tts
import httpx
import miniaudio
import websockets
from fastapi import APIRouter, Request, Response, WebSocket
from twilio.rest import Client as TwilioClient
from twilio.twiml.voice_response import Connect, Stream, VoiceResponse

from db.connection import get_pool

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Config — all from environment, zero hard-coded values
# ---------------------------------------------------------------------------
TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
DEEPGRAM_API_KEY    = os.getenv("DEEPGRAM_API_KEY")
GROQ_API_KEY        = os.getenv("GROQ_API_KEY")

# Voice for edge-tts — en-US-AriaNeural is warm, professional, free
TTS_VOICE = os.getenv("TTS_VOICE", "en-US-AriaNeural")

# Groq model — llama-3.3-70b: best quality/cost ratio on Groq
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# ---------------------------------------------------------------------------
# Deepgram params — Nova-2 English, endpointing at 1500ms silence.
#
# interim_results=true is REQUIRED for barge-in detection. Deepgram sends
# partial transcripts the instant the partner starts making sound — well
# before their full utterance is finished. We use these interim results as
# the trigger to immediately stop the agent's TTS playback. Without this,
# the agent has zero signal that the partner has started talking until
# their entire sentence is done, by which point it has already spoken over them.
# ---------------------------------------------------------------------------
DEEPGRAM_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2"
    "&language=en"
    "&punctuate=true"
    "&smart_format=true"
    "&interim_results=true"
    "&endpointing=1100"
    "&encoding=mulaw"
    "&sample_rate=8000"
    "&channels=1"
)

# Max tokens for LLM reply — keep short for voice (spoken responses should be concise)
LLM_MAX_TOKENS = 180

# Silence timeout: if NO EXCHANGE has happened at all (zero partner
# utterances since the call connected) for this many seconds, play a
# single nudge. This only fires once, only on a totally dead call —
# NOT after every natural pause once a conversation is underway, which
# was the bug causing the agent to "recite the script" mid-conversation.
#
# Shortened from 28s to 6s per explicit script requirement: right after
# the opening line ("...am I speaking with X?"), a real person checks in
# much sooner than 28 seconds if there's no response — "Hello? Are you
# there?" — matching natural phone behaviour. Still fires ONLY when zero
# exchange has happened at all (see _silence_nudge_loop below), so it can
# never fire mid-conversation on a normal thinking-pause.
SILENCE_NUDGE_S = 6

# ── Call recording config ────────────────────────────────────────────────
# Recordings saved locally as 8kHz mono 16-bit PCM WAV files — one file per
# call, mixing both the partner's audio and the agent's TTS audio into a
# single track in real chronological order (not two separate files).
#
# CRITICAL: this directory MUST live OUTSIDE the project root when running
# with `uvicorn --reload`. uvicorn's reloader (watchfiles) watches the
# entire project directory recursively. Each WAV writeframes() call during
# a live call is a file write — if recordings/ is inside the watched tree,
# every single audio frame write triggers a reload cycle (visible as
# "1 change detected" spamming the log every ~400ms during calls). A
# reload tears down and recreates module-level state mid-call, which is
# what was actually causing the garbled "agent talking to itself" behavior
# — NOT a Deepgram or echo-rejection bug.
#
# Default: a sibling directory one level OUTSIDE the project, so it is
# never inside uvicorn's watched path regardless of where the server is
# started from. Override with RECORDINGS_DIR if you want a different
# location — just keep it outside the project root.
_DEFAULT_RECORDINGS_DIR = Path(__file__).resolve().parent.parent.parent / "gtm_call_recordings"
_RECORDINGS_DIR = Path(os.getenv("RECORDINGS_DIR", str(_DEFAULT_RECORDINGS_DIR)))
_RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
RECORDING_ENABLED: bool = os.getenv("RECORDING_ENABLED", "true").lower() == "true"

logger.info("Call recordings will be saved to: %s (outside project root, reload-safe)", _RECORDINGS_DIR)

# Set once at startup by voice_agent/tunnel.py
PUBLIC_HOST: str = ""

# In-memory call registry keyed by Twilio CallSid
_calls: dict[str, dict] = {}


def set_public_host(host: str) -> None:
    global PUBLIC_HOST
    PUBLIC_HOST = host
    logger.info("Voice agent public host set to: %s", host)


# ---------------------------------------------------------------------------
# Intent classifiers — used by _handle_utterance to detect special responses
# ---------------------------------------------------------------------------

# Hard stop signals — partner wants no further contact
_DNC_PATTERNS = re.compile(
    r"\b(not interested|stop|no thanks|don't call|do not call|remove me|"
    r"unsubscribe|leave me alone|never call|go away|hang up)\b",
    re.IGNORECASE,
)

# Escalation triggers — hand off to human immediately
_ESCALATION_PATTERNS = re.compile( 
    r"\b(commission|revenue share|percentage|contract|legal|lawyer|"
    r"privacy|gdpr|data protection|complaint|frustrated|angry|"
    r"speak to a human|speak to someone|real person|your manager|"
    r"head office|enterprise|multiple locations|chain|franchise)\b",
    re.IGNORECASE,
)

# Positive interest — move into qualification flow
_INTEREST_PATTERNS = re.compile(
    r"\b(tell me more|how does it work|interested|send details|sounds good|"
    r"yes|sure|okay|go ahead|what is aarna|explain|how do I|sign up|onboard)\b",
    re.IGNORECASE,
)

# Neutral / busy — pause and schedule follow-up
_BUSY_PATTERNS = re.compile(
    r"\b(call back|call later|busy|not now|another time|next week|"
    r"send an email|whatsapp me|send a message)\b",
    re.IGNORECASE,
)

# Agent farewell — signals end of call when spoken by the LLM.
# Word-boundary match only — "bye" inside "nearby" will never fire.
# Compiled once at module level, not inside the hot per-utterance path.
_FAREWELL_PATTERNS = re.compile(
    r"\b("
    r"have a wonderful day"
    r"|goodbye"
    r"|take care"
    r"|farewell"
    r"|have a great day"
    r"|have a good day"
    r"|talk soon"
    r"|speak soon"
    r"|all the best"
    r"|wish you well"
    r"|nice talking"
    r"|pleasure speaking"
    r")\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# System prompt — built from Aarna AI Behaviour & Outreach Script Framework
# ---------------------------------------------------------------------------

# Category-specific personalized pitch questions — used in STEP 3 instead
# of the generic "Do you currently list your experiences with any other
# platforms?" line. Matched by substring against partners.category, so
# "Desert Safari & Adventure" still matches "desert safari" below.
_CATEGORY_PITCH_TEMPLATES: list[tuple[str, str]] = [
    ("restaurant", "Would love to know more about {partner} — what's the concept and cuisine like? "
                    "Do you currently list {partner} with any other platforms, and would you be open "
                    "to exploring Aarna as well?"),
    ("hotel", "I'd love to hear more about {partner} — what's the guest experience like? Are you "
              "currently distributing your rooms or packages through any other travel platforms, "
              "and would you be open to exploring Aarna alongside them?"),
    ("resort", "I'd love to hear more about {partner} — what's the guest experience like? Are you "
               "currently distributing your rooms or packages through any other travel platforms, "
               "and would you be open to exploring Aarna alongside them?"),
    ("desert safari", "Tell me a bit about the experiences you run at {partner} — is it mainly "
                       "desert safaris, or other adventure activities too? Do you currently list "
                       "these with any other platforms, and would you be open to exploring Aarna "
                       "as well?"),
    ("adventure", "Tell me a bit about the experiences you run at {partner}. Do you currently list "
                  "these with any other platforms, and would you be open to exploring Aarna as well?"),
    ("water sport", "What kind of water activities does {partner} offer? Are these currently "
                     "bookable through any other platforms, and would you be open to listing them "
                     "on Aarna too?"),
    ("spa", "I'd love to hear about the treatments and experience at {partner}. Do you currently "
            "list {partner} with any wellness or travel booking platforms, and would you be open "
            "to exploring Aarna as well?"),
    ("wellness", "I'd love to hear about the experience at {partner}. Do you currently list "
                 "{partner} with any wellness or travel booking platforms, and would you be open "
                 "to exploring Aarna as well?"),
    ("tour", "What kind of tours or itineraries does {partner} typically run? Are you currently "
             "distributing these through any other travel platforms, and would you be open to "
             "exploring Aarna as well?"),
]

_GENERIC_PITCH_TEMPLATE = (
    "Do you currently list your experiences with any other platforms, and would you be open to "
    "exploring Aarna as well?"
)


def _personalized_pitch_question(partner_name: str, category: str, company_synopsis: str) -> str:
    """
    Build the STEP 3 pitch question. Prefers company_synopsis (a real,
    enrichment-sourced description) when available, falls back to a
    category-matched template, falls back to the fully generic question
    only when neither is known. Never invents details not actually given.
    """
    partner = partner_name or "your business"

    if company_synopsis:
        return (
            f"I looked into {partner} — {company_synopsis}. I'd love to know a bit more about "
            f"how things are going there. Do you currently list {partner} with any other "
            f"platforms, and would you be open to exploring Aarna as well?"
        )

    if category:
        cat_lower = category.lower()
        for keyword, template in _CATEGORY_PITCH_TEMPLATES:
            if keyword in cat_lower:
                return template.format(partner=partner)

    return _GENERIC_PITCH_TEMPLATE


def _system_prompt(
    partner_name: str,
    digitisation: str,
    category: str = "",
    company_synopsis: str = "",
) -> str:
    """
    Build the system prompt for the live call.

    Rewritten to be SHORT and CRISP per explicit instruction — the agent
    was speaking too much immediately after the partner picked up, with a
    long multi-step scripted pitch (stats, pricing, launch-partner framing)
    that added no value early in the call. New flow:

        (opening already spoken via TwiML) ->
        AI disclosure + confirm who I'm speaking with ->
        one to-the-point PERSONALIZED question about their business ->
        offer + schedule a human-agent call ->
        close

    AI disclosure is back with EXACT required wording (previously removed
    per a different instruction — that decision is superseded here).

    Explicit anti-hallucination guardrail added: the agent must never
    invent pricing, commission rates, or other specific details it wasn't
    given — always defer unknowns to the human agent on the scheduled call
    rather than guessing or improvising an answer. This now ALSO covers
    company_synopsis: the agent must never invent facts about the specific
    business beyond what's actually provided in company_synopsis or what
    the supplier says on the call themselves.

    digitisation: one of 'digitised', 'semi', 'hyperlocal'
      - digitised:   hotels, DMCs, large tour operators, attractions
      - semi:        local tour ops, boat rentals, desert safari, wellness
      - hyperlocal:  home chefs, workshops, art studios, boutique fitness

    category: business vertical (e.g. "Restaurant", "Hotel & Resort") —
      drives the personalized STEP 3 pitch question. Optional.
    company_synopsis: factual 1-2 sentence description of what this
      specific business does, from real enrichment data only. Optional —
      most partners won't have this until an enrichment step generates it.
    """

    pitch_question = _personalized_pitch_question(partner_name, category, company_synopsis)

    # Tone and framing vary by digitisation tier per the document
    if digitisation == "digitised":
        tone_guidance = (
            "Use a professional partnership tone. Mention distribution, bookings, "
            "and visibility. This supplier understands B2B language."
        )
    elif digitisation == "semi":
        tone_guidance = (
            "Use a friendly business expansion tone. Keep language simple. "
            "Focus on getting more bookings and being discovered. "
            "Avoid corporate jargon. WhatsApp follow-up is preferred by this segment."
        )
    else:  # hyperlocal
        tone_guidance = (
            "Use a highly conversational, human tone. Focus on 'getting discovered' "
            "and helping local businesses grow. Avoid all tech or corporate language. "
            "Explain everything simply as if talking to a small business owner."
        )

    return f"""You are Sania, an AI partnership assistant for Aarna, part of the
Mondee Group. You are on a live outbound voice call with
{partner_name or 'a UAE experience or activity supplier'}.


PERSONA:
- A calm, warm, efficient assistant helping the Aarna partnerships team
- You respect the supplier's time above all else — get to the point fast
- You are NOT a sales bot reading a pitch — you ask, you listen, you move on

CRITICAL — GROUND EVERY REPLY IN WHAT THEY ACTUALLY SAID:
The supplier's most recent message is the single most important input for
your reply. Never give a generic or pre-scripted-sounding response. Always
react to specific words or details they just used. If you find yourself
about to say something that would make sense regardless of what they just
said, stop and rewrite it to reference their actual words.

CRITICAL — NEVER HALLUCINATE:
If the supplier asks something you don't have a clear, explicit answer for
in this prompt (exact pricing, commission percentages, contract terms,
specific numbers, launch dates, or anything else not stated here), do NOT
invent an answer or guess. Say something like: "That's a great question —
I'll make sure our partnership lead covers that on your call." Then move
on. Never fill a gap with a plausible-sounding but made-up detail.

This deferral phrase is ONLY for genuine unanswered questions. It must
NEVER be used in reply to a plain scheduling statement — a date, a time,
"yes that works", "tomorrow at 2", or similar. Those get a short, direct
confirmation ONLY (e.g. "Perfect, 2PM tomorrow works — I'll send the
invite."), with no deferral phrase attached, even if you used the
deferral phrase earlier in this same call for a different, unrelated
question. Each reply must be judged only on what was JUST said, not on
carrying over a pattern from a previous turn.

═══════════════════════════════════════════════════════════════
CALL FLOW — SHORT AND CRISP. NO LONG EXPLANATIONS AT ANY STEP.
═══════════════════════════════════════════════════════════════
The opening line has ALREADY been spoken before you say anything:
"Hi, this is Sania calling from Aarna, our travel management platform.
Am I speaking with [name/company]?"

If there was no response at all, a short "Hello? Are you there?" check-in
has ALSO already played automatically — you don't need to say this
yourself, it happens before you get involved.

Your job starts with THEIR response to the opening. Follow this flow,
one step at a time, waiting for and reacting to their actual answer
before moving to the next step. Every step is ONE short sentence or two
at most — never explain, elaborate, or pitch at length.

STEP 1 — Confirm you're understood, then check timing.
- If their response is unclear, garbled, or asks you to repeat yourself
  ("sorry, who?", "come again?", "can't hear you"), repeat the opening
  briefly and add the good-time check: "This is Sania from Aarna — is
  now a good time to speak?" Do this only once; if still unclear a
  second time, politely end the call.
- If they confirm they can hear you (a name, "yes", "speaking", etc.),
  ask directly: "Is now a good time to speak?"

STEP 2 — Branch on their answer to the good-time check.
- If NOT a good time (busy, later, no): do not push forward. Offer a
  calendar invite instead: "No problem — would it help if I sent a
  calendar invite so we can find a better time?" Capture a preferred
  day/time if they give one, otherwise just confirm you'll send
  something by email, then close warmly.
- If it IS a good time: re-confirm who you're speaking with in one
  breath, disclose AI status, and state your purpose — all in one short
  turn. Say something like: "Great — am I speaking with [name]? Just so
  you know, I'm an AI assistant for Aarna. I'd like to speak with you
  about a partnership opportunity with Aarna." Keep this to one turn,
  then move to STEP 3.

STEP 3 — Gauge INTEREST directly — cut to the point, no pitch yet.
Ask this question, adapted naturally in your own words but keeping its
exact meaning and specifics — do not revert to a generic question about
"your experiences" if a personalized one is given below:

"{pitch_question}"

Listen for genuine interest vs disinterest in their answer — this is the
single most important branch in the call:
- If they sound INTERESTED (curious, asking questions, willing to hear
  more): give ONE brief, concrete sentence about what Aarna does (no
  stats, no pricing — save that for the human call), then move straight
  then move straight to STEP 4 (schedule). Do not linger here or add a second sentence.
- If they sound NOT INTERESTED (dismissive, "not right now", "we're
  fine", no genuine curiosity): do not push or re-pitch. Acknowledge
  politely and offer once to send information by email instead of
  continuing to press for a call. If they decline that too, close the
  call warmly without scheduling anything.
- If what they describe is CLEARLY unrelated to UAE experiences/
  activities (software, retail, unrelated professional services,
  manufacturing, etc.), this is a fit problem, not an interest problem —
  say something like: "Ah, that's not quite the right fit for Aarna,
  which focuses on UAE experience and activity providers — apologies
  for the mix-up, and thanks for your time! Have a wonderful day!" Then
  end the call. Ask one clarifying question first if genuinely unsure
  rather than guessing either way.

STEP 4 — Offer and schedule the human-agent call. Say something like:
"I'd like to set up a short call with one of our partnership leads to
walk you through everything — would sometime this week or next work?"
Once they agree, ask: "What day and time works best for you?" Capture
their answer clearly and repeat it back to confirm you heard it right.
When they state a date/time, this is a plain scheduling answer, not a
question — respond with a direct confirmation only, never the deferral
phrase from the anti-hallucination rule above.

STEP 5 — Confirm and close. Say something like: "Perfect, I'll send a
calendar invite to your email. Have a wonderful day!"

TONE:
- Short. Crisp. To the point. Every reply 1 sentence, occasionally 2.
- One question per turn — never stack multiple questions together.
- {tone_guidance}

WHAT TO AVOID ALWAYS:
- Long replies, explanations, or pitches of any kind
- Mentioning specific stats, numbers, pricing, or commission terms —
  defer all of that to the human agent call, every time
- Corporate jargon, hard selling, pressure tactics
- Filler phrases like "Certainly!" or "Absolutely!" or "Great question!"
- Asking more than ONE question per turn
- Continuing to talk once the supplier has answered — move to the next
  step immediately rather than adding extra commentary

═══════════════════════════════════════════════════════════════
INTERRUPTION HANDLING
═══════════════════════════════════════════════════════════════
You are speaking over a live phone line. The supplier may start talking
while you are mid-sentence. If you are told you were just interrupted:
- Stop immediately, do not finish your previous thought
- Apologise in 3-5 words max: "Sorry, go ahead." or "Apologies, please continue."
- Then listen to and directly respond to what they actually said
- Do NOT repeat or re-explain what you were saying before the interruption
- Do NOT restart your previous sentence from the beginning
Never speak in long uninterrupted monologues. Keep every turn short enough
that the supplier has natural room to jump in.

RESPONSE HANDLING (applies at any point in the call flow above):
- Positive interest: move to the next step in the flow naturally.
- Neutral/busy ("later", "busy", "send details"): offer to schedule the
  human-agent call for a better time, or ask their preferred follow-up
  channel (email/WhatsApp). Do not push.
- Suspicious/confused ("who are you?", "where did you get my number?"):
  Clarify transparently that their business was found through public
  UAE tourism directories. Reduce pressure immediately.
- Soft no to a specific offer ("No.", "No thanks", "Not right now"):
  Do NOT end the call. Acknowledge politely and try one alternative —
  e.g. if they decline the call, offer to just send information by email
  or WhatsApp instead.
- Hard no ("not interested", "stop", "don't call again"):
  Say exactly: "Understood — thanks for letting me know. We won't reach
  out further. Wishing your business continued success." Then end the call.

ESCALATION — say the following and end the call if the conversation involves:
- Commission negotiation, revenue share, or contract specifics
- Legal, privacy, or data concerns
- Complaints or frustration
- Enterprise or multi-location operators
- Any topic you are unsure about
Say: "That's a great question for our partnership lead — I'll make sure
they cover that when we set up your call."

TOPIC BOUNDARY:
Stay within the call flow above and Aarna's platform, onboarding, and
partnership model. If the supplier brings up something entirely
unrelated, redirect briefly and warmly back to the current step of the
call flow.

UAE CULTURAL RULES:
- Polite English only. No slang.
- Multicultural-neutral. Avoid assumptions about language or background.
- Never aggressive. Never repeat follow-ups after a clear no.
- Respect that business owners are busy.

FAREWELL — when ending the call, use ONLY one of these phrases and nothing else:
"Have a wonderful day!", "goodbye", "take care", "talk soon"
NEVER use these mid-conversation."""


# ---------------------------------------------------------------------------
# Audio helpers — all pure Python / stdlib, no ffmpeg
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# mu-law <-> PCM16 codec — uses stdlib `audioop`, the C-implemented reference
# G.711 codec. This replaces a previous hand-written pure-Python version
# which had a bit-ordering bug in the encoder (verified by round-trip
# testing against audioop: original samples were coming back wildly wrong,
# e.g. 16000 decoding to ~2000). audioop is correct, fast (C extension,
# not Python loops), and available in this project's Python 3.10 runtime
# without needing the audioop-lts backport (only required on 3.13+).
# ---------------------------------------------------------------------------
import audioop


def _pcm16_to_mulaw(pcm_bytes: bytes) -> bytes:
    """
    Convert 16-bit signed PCM samples to 8-bit G.711 mu-law.
    Twilio media stream expects: mulaw, 8 kHz, mono, base64-encoded.
    """
    if not pcm_bytes:
        return b""
    return audioop.lin2ulaw(pcm_bytes, 2)


def _mulaw_to_pcm16(mulaw_bytes: bytes) -> bytes:
    """
    Convert 8-bit G.711 mu-law back to 16-bit signed PCM.
    Used for call recording — decodes the raw mulaw bytes flowing through
    the WebSocket into a standard PCM16 WAV-compatible format.
    """
    if not mulaw_bytes:
        return b""
    return audioop.ulaw2lin(mulaw_bytes, 2)


async def _tts_to_mulaw(text: str) -> bytes:
    """
    Synthesise text → edge-tts MP3 → PCM16 @ 8 kHz mono → mulaw bytes.

    edge-tts streams MP3 chunks; we accumulate then decode in one pass
    with miniaudio (handles resample from 24 kHz → 8 kHz internally).
    Total added latency: ~300-600 ms for a short sentence (network + decode).
    """
    communicate = edge_tts.Communicate(text, voice=TTS_VOICE)
    mp3_chunks: list[bytes] = []

    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            mp3_chunks.append(chunk["data"])

    if not mp3_chunks:
        logger.warning("TTS: empty audio for text: %r", text[:60])
        return b""

    mp3_bytes = b"".join(mp3_chunks)

    # miniaudio.decode handles: MP3 parse + resample to 8000 Hz + mono downmix
    decoded = miniaudio.decode(
        mp3_bytes,
        output_format=miniaudio.SampleFormat.SIGNED16,
        nchannels=1,
        sample_rate=8000,
    )
    pcm_bytes = bytes(decoded.samples)
    return _pcm16_to_mulaw(pcm_bytes)


def _mulaw_to_twilio_message(mulaw_bytes: bytes, stream_sid: str) -> str:
    """
    Wrap mulaw audio bytes in a Twilio media WebSocket message (JSON string).
    This is what we send *back* to Twilio so the caller hears the TTS reply.
    """
    return json.dumps({
        "event": "media",
        "streamSid": stream_sid,
        "media": {
            "payload": base64.b64encode(mulaw_bytes).decode("ascii"),
        },
    })


class _CallRecorder:
    """
    Records both audio directions (partner mic, agent TTS) into a single
    mono WAV file, properly MIXED rather than naively concatenated.

    Why mixing matters: partner audio and agent audio happen in overlapping
    real time — Twilio delivers each side as a continuous stream of 20ms
    mulaw frames. If we simply appended whichever side's bytes arrived
    first to one track, the result would be garbled and out of sync
    (you'd hear partner audio, then a chunk of silence, then agent audio,
    never both at once even during real overlap/barge-in moments).

    Approach: maintain two small PCM16 sample buffers (one per direction).
    Every time EITHER side delivers a chunk, we top up that side's buffer.
    A periodic flush takes the next _FRAME_SAMPLES worth of audio from
    BOTH buffers (zero-filling whichever side has less queued), sums them
    sample-by-sample with clipping, and writes ONE mixed frame to disk.
    This keeps memory flat (only ever holds a few hundred ms of buffered
    audio, never the whole call) while producing a correctly mixed,
    chronologically accurate recording — including real overlapping
    speech during barge-in moments.
    """

    # 20ms at 8kHz = 160 samples — matches Twilio's native frame size,
    # so flushing at this granularity introduces no extra latency or
    # buffering beyond what Twilio already does internally.
    _FRAME_SAMPLES = 160

    def __init__(self, call_sid: str):
        self.call_sid = call_sid
        self.path = _RECORDINGS_DIR / f"{call_sid}.wav"
        self._wav: "wave.Wave_write | None" = None
        self._partner_buf = bytearray()   # PCM16 bytes, partner side
        self._agent_buf   = bytearray()   # PCM16 bytes, agent side
        self._frames_written = 0
        self._lock = asyncio.Lock()

    def _ensure_open(self) -> None:
        if self._wav is None:
            self._wav = wave.open(str(self.path), "wb")
            self._wav.setnchannels(1)
            self._wav.setsampwidth(2)   # 16-bit PCM
            self._wav.setframerate(8000)

    async def write_partner_mulaw(self, mulaw_bytes: bytes) -> None:
        """Queue partner audio for mixing. Decoded to PCM16 immediately."""
        if not mulaw_bytes:
            return
        async with self._lock:
            self._partner_buf.extend(_mulaw_to_pcm16(mulaw_bytes))
            await self._flush_ready_frames()

    async def write_agent_mulaw(self, mulaw_bytes: bytes) -> None:
        """Queue agent (TTS) audio for mixing. Decoded to PCM16 immediately."""
        if not mulaw_bytes:
            return
        async with self._lock:
            self._agent_buf.extend(_mulaw_to_pcm16(mulaw_bytes))
            await self._flush_ready_frames()

    async def _flush_ready_frames(self) -> None:
        """
        Mix and write out as many complete frames as both buffers can
        support, zero-padding whichever side is currently shorter.
        Called after every write — keeps buffers from growing unbounded.
        """
        frame_bytes = self._FRAME_SAMPLES * 2  # 2 bytes per PCM16 sample

        # Only flush while at least ONE side has a full frame ready —
        # this prevents a fast-talking side from racing far ahead of a
        # quiet side while still keeping latency low (max ~20ms held back).
        while len(self._partner_buf) >= frame_bytes or len(self._agent_buf) >= frame_bytes:
            p_chunk = bytes(self._partner_buf[:frame_bytes]) if self._partner_buf else b""
            a_chunk = bytes(self._agent_buf[:frame_bytes]) if self._agent_buf else b""

            # Zero-pad the shorter side up to a full frame
            if len(p_chunk) < frame_bytes:
                p_chunk += b"\x00" * (frame_bytes - len(p_chunk))
            if len(a_chunk) < frame_bytes:
                a_chunk += b"\x00" * (frame_bytes - len(a_chunk))

            mixed = self._mix_pcm16(p_chunk, a_chunk)

            self._ensure_open()
            self._wav.writeframes(mixed)
            self._frames_written += 1

            del self._partner_buf[:frame_bytes]
            del self._agent_buf[:frame_bytes]

    @staticmethod
    def _mix_pcm16(a: bytes, b: bytes) -> bytes:
        """
        Sum two equal-length PCM16 buffers with correct clipping.
        Uses audioop.add — the C-implemented stdlib mixer, which is both
        correct (no hand-rolled overflow bugs) and fast (not a Python loop
        per sample, matters since this runs continuously for call duration).
        """
        return audioop.add(a, b, 2)

    async def close(self) -> "str | None":
        """Flush any remaining buffered audio, finalise the WAV, return its path."""
        async with self._lock:
            # Final flush — pad out whatever partial frame remains so we
            # don't lose the last <20ms of audio from either side.
            frame_bytes = self._FRAME_SAMPLES * 2
            if self._partner_buf or self._agent_buf:
                p_chunk = bytes(self._partner_buf) + b"\x00" * max(0, frame_bytes - len(self._partner_buf))
                a_chunk = bytes(self._agent_buf)   + b"\x00" * max(0, frame_bytes - len(self._agent_buf))
                p_chunk = p_chunk[:frame_bytes]
                a_chunk = a_chunk[:frame_bytes]
                mixed = self._mix_pcm16(p_chunk, a_chunk)
                self._ensure_open()
                self._wav.writeframes(mixed)
                self._frames_written += 1
                self._partner_buf.clear()
                self._agent_buf.clear()

            if self._wav is not None:
                self._wav.close()
                self._wav = None

        if self._frames_written == 0:
            try:
                if self.path.exists():
                    self.path.unlink()
            except Exception:
                pass
            return None
        return str(self.path)


def _clear_message(stream_sid: str) -> str:
    """
    Twilio 'clear' event — tells Twilio to immediately discard any buffered
    audio it has not yet played out. This is essential for barge-in: simply
    stopping our chunk-sending loop is not enough, because Twilio may have
    already buffered several chunks ahead. Without this clear event, the
    caller would still hear a fraction of a second of stale audio after we
    detect the interruption.
    """
    return json.dumps({
        "event": "clear",
        "streamSid": stream_sid,
    })


# ---------------------------------------------------------------------------
# LLM reply generation (Groq, streaming)
# ---------------------------------------------------------------------------

# Sentence boundary detector — splits on ./!/? followed by space or end of string.
# Used to flush complete sentences to TTS as soon as they're ready, rather
# than waiting for the entire LLM reply to finish generating.
_SENTENCE_END_RE = re.compile(r"(?<=[.!?])\s+")


async def _llm_reply_stream(history: list[dict]):
    """
    Send conversation history to Groq and YIELD each complete sentence as
    soon as it is available, instead of waiting for the full reply.

    This is the core latency fix: previously we waited for the ENTIRE Groq
    reply (often 1-2s for a 2-sentence response) before TTS even started.
    Now the first sentence goes to TTS the moment Groq emits the period
    that ends it — TTS for sentence 1 plays while Groq is still generating
    sentence 2. This eliminates almost all of the "void" between the
    partner finishing speaking and the agent's voice starting.

    Yields: str (one complete sentence at a time, trailing whitespace stripped)
    """
    if not GROQ_API_KEY:
        yield "I'm sorry, I'm having a technical issue. Let me call you back shortly."
        return

    payload = {
        "model": GROQ_MODEL,
        "messages": history,
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": 0.4,
        "stream": True,
    }

    buffer = ""

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            async with client.stream(
                "POST",
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line[6:]
                    if data.strip() == "[DONE]":
                        break
                    try:
                        delta = json.loads(data)["choices"][0]["delta"].get("content", "")
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
                    if not delta:
                        continue

                    buffer += delta

                    # Check if buffer now contains one or more complete sentences
                    parts = _SENTENCE_END_RE.split(buffer)
                    if len(parts) > 1:
                        # All parts except the last are complete sentences —
                        # the last part is still being generated, keep it buffered.
                        for sentence in parts[:-1]:
                            sentence = sentence.strip()
                            if sentence:
                                yield sentence
                        buffer = parts[-1]

        # Flush whatever remains after the stream ends (final sentence,
        # or a reply with no terminal punctuation at all)
        remainder = buffer.strip()
        if remainder:
            yield remainder

    except Exception as exc:
        logger.error("LLM streaming error: %s", exc)
        if not buffer.strip():
            yield "I apologise, I had a brief technical issue. Could you repeat that?"
        else:
            yield buffer.strip()


async def _llm_reply(history: list[dict]) -> str:
    """
    Non-streaming convenience wrapper — collects the full reply.
    Used only where the full text is needed upfront (e.g. nowhere in the
    live call loop anymore, kept for compatibility / testing).
    """
    parts = []
    async for sentence in _llm_reply_stream(history):
        parts.append(sentence)
    return " ".join(parts).strip()


# ---------------------------------------------------------------------------
# Public entry point — called by outreach_node
# ---------------------------------------------------------------------------

async def place_call(
    to: str,
    script: str,
    mission: str,
    partner_id: Optional[int] = None,
    partner_name: Optional[str] = None,
    partner_email: Optional[str] = None,
    contact_name: Optional[str] = None,
    digitisation: str = "semi",
    category: Optional[str] = None,
    company_synopsis: Optional[str] = None,
    timeout_s: int = 300,
) -> dict:
    """
    Place an outbound call. Blocks until the full call completes
    (conversation + post-call summary) or timeout_s seconds elapse.

    category: partner's business vertical (e.g. "Restaurant", "Hotel &
        Resort", "Desert Safari", "Water Sports", "Spa & Wellness", "Tour
        Operator"), used to personalize both the opening line context and
        the mid-call pitch question instead of asking generically about
        "your experiences". Falls back to generic phrasing if not given.
    company_synopsis: 1-2 sentence factual description of what this
        specific business does, sourced from real enrichment data (never
        invented). Optional — most partners won't have this yet since no
        enrichment step currently generates it. When present, it's used
        to open the personalized business question; when absent, the
        category alone still drives a personalized (but lighter) question
        rather than falling all the way back to fully generic phrasing.

    Returns:
        {
            "call_sid":  str,
            "status":    str,   # completed | no-answer | busy | failed | timeout
            "duration_s": int,
            "summary":   dict | None,
        }
    """
    if not PUBLIC_HOST:
        return {"call_sid": None, "status": "error", "duration_s": 0, "summary": None,
                "error": "Voice agent tunnel not initialised"}

    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return {"call_sid": None, "status": "error", "duration_s": 0, "summary": None,
                "error": "Twilio credentials not configured"}

    twilio = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    done_event = asyncio.Event()

    try:
        call = await asyncio.to_thread(
            twilio.calls.create,
            url=f"https://{PUBLIC_HOST}/twilio/outbound",
            status_callback=f"https://{PUBLIC_HOST}/twilio/status",
            status_callback_method="POST",
            status_callback_event=["completed", "no-answer", "busy", "failed"],
            to=to,
            from_=TWILIO_PHONE_NUMBER,
        )
    except Exception as exc:
        logger.error("Failed to place call to %r: %s", to, exc)
        return {"call_sid": None, "status": "error", "duration_s": 0, "summary": None,
                "error": str(exc)}

    call_sid = call.sid
    _calls[call_sid] = {
        "script": script,
        "mission": mission,
        "partner_id": partner_id,
        "partner_name": partner_name or "",
        "partner_email": partner_email or "",   # used for calendar invite
        "contact_name": contact_name or "",      # specific person, if known — used in opening line
        "digitisation": digitisation,   # drives tone: digitised / semi / hyperlocal
        "category": category or "",               # drives personalized pitch question
        "company_synopsis": company_synopsis or "",  # factual 1-2 sentence business description, if known
        "to": to,
        "history": [],          # full conversation history for LLM context
        "transcript": [],       # flat list of {speaker, text, ts} for DB
        "done_event": done_event,
        "result": None,
        "stream_sid": None,     # Twilio StreamSid, set when WS opens
        "ai_disclosed": False,
        "dnc_requested": False,
        "escalation_requested": False,
        "_call_ended": False,
        "recording_path": None,   # set once the call ends and WAV is finalised
    }

    await _db_upsert_call(
        call_sid,
        partner_id=partner_id,
        partner_name=partner_name,
        mission=mission,
        to_number=to,
        status="initiated",
        started_at=_utc_naive(),
    )

    logger.info("Call placed SID=%s to=%s partner=%r", call_sid, to, partner_name)

    try:
        await asyncio.wait_for(done_event.wait(), timeout=timeout_s)
    except asyncio.TimeoutError:
        logger.warning("Call %s timed out after %ds", call_sid, timeout_s)
        result = {"call_sid": call_sid, "status": "timeout", "duration_s": timeout_s, "summary": None}
        _calls.pop(call_sid, None)
        return result

    result = (_calls.pop(call_sid, {}) or {}).get("result") or {
        "call_sid": call_sid, "status": "unknown", "duration_s": 0, "summary": None,
    }
    return result


# ---------------------------------------------------------------------------
# Twilio webhook: call connected → TwiML opening line + stream connect
# ---------------------------------------------------------------------------

@router.api_route("/twilio/outbound", methods=["GET", "POST"])
async def twilio_outbound(request: Request):
    """
    Twilio calls this URL when the partner picks up.
    We respond with TwiML that:
      1. Says the opening line (Twilio Polly TTS — instant, no latency)
      2. Opens a bidirectional media stream to our WebSocket
    The WS handler takes over from there for the full conversation.
    """
    form = await request.form()
    call_sid = form.get("CallSid", "unknown")
    meta = _calls.get(call_sid, {})

    await _db_upsert_call(call_sid, status="connected", from_number=form.get("From", ""))
    logger.info("Call connected SID=%s", call_sid)

    response = VoiceResponse()

    # Opening line — HARDCODED, exact wording per explicit instruction.
    # Do not paraphrase or let the LLM regenerate this; it's spoken via
    # Twilio Polly before the LLM/WS conversation even starts.
    #
    # Full form (both name and company known):
    #   "Hi, I am Sania, an AI assistant from Aarna - a travel management
    #    platform. Am I talking with {client_name} from {company_name}?
    #    Are you the right person to discuss the partnership with you
    #    for {company_name}?"
    #
    # Degrades gracefully when contact_name and/or partner_name are
    # missing, rather than saying "None" or leaving an awkward blank.
    contact_name = meta.get("contact_name") or ""
    partner_name = meta.get("partner_name") or ""

    if contact_name and partner_name:
        opening = (
            f"Hi, I am Sania, an AI assistant from Aarna - a travel "
            f"management platform. Am I talking with {contact_name} from "
            f"{partner_name}? Are you the right person to discuss the "
            f"partnership with you for {partner_name}?"
        )
    elif partner_name:
        opening = (
            f"Hi, I am Sania, an AI assistant from Aarna - a travel "
            f"management platform. Am I speaking with the right person "
            f"at {partner_name}? Are you the person to discuss the "
            f"partnership with you for {partner_name}?"
        )
    else:
        opening = (
            "Hi, I am Sania, an AI assistant from Aarna - a travel "
            "management platform. Am I speaking with the right person "
            "to discuss a potential partnership?"
        )

    opening = meta.get("script") or opening
    response.say(opening, voice="Polly.Joanna-Neural", language="en-US")

    # Pause briefly so the greeting finishes before the stream opens
    response.pause(length=1)

    # Open bidirectional media stream — Twilio will send AND receive audio on this WS
    connect = Connect()
    stream = Stream(url=f"wss://{PUBLIC_HOST}/ws/audio/{call_sid}")
    stream.parameter(name="callSid", value=call_sid)
    connect.append(stream)
    response.append(connect)

    return Response(content=str(response), media_type="application/xml")


# ---------------------------------------------------------------------------
# Twilio webhook: call status events
# ---------------------------------------------------------------------------

@router.api_route("/twilio/status", methods=["POST"])
async def twilio_status(request: Request):
    form = await request.form()
    call_sid = form.get("CallSid", "")
    status   = form.get("CallStatus", "")
    duration = int(form.get("CallDuration", "0") or 0)

    logger.info("Call status SID=%s status=%s duration=%ds", call_sid, status, duration)

    if call_sid in _calls:
        _calls[call_sid]["status"]   = status
        _calls[call_sid]["duration"] = duration

    await _db_upsert_call(
        call_sid,
        status=status,
        duration_s=duration,
        ended_at=_utc_naive(),
    )

    # Calls that never connected: finalise immediately (WS never opens)
    if status in ("no-answer", "busy", "failed") and call_sid in _calls:
        meta = _calls[call_sid]
        meta["result"] = {
            "call_sid": call_sid,
            "status": status,
            "duration_s": duration,
            "summary": None,
        }
        meta["done_event"].set()

    return Response(status_code=204)


# ---------------------------------------------------------------------------
# WebSocket: bidirectional audio bridge — the core conversation loop
# ---------------------------------------------------------------------------

@router.websocket("/ws/audio/{call_sid}")
async def audio_stream(ws: WebSocket, call_sid: str):
    """
    Bidirectional conversation loop with TRUE barge-in support:

      Twilio (mulaw in) ──► Deepgram (STT, interim + final) ──► Groq (LLM) ──► edge-tts (TTS)
                                  │                                                  │
                          interim result during                                     │
                          agent speech = barge-in                                   │
                          signal → cuts TTS NOW                                     │
                                                                                     ▼
      Twilio (mulaw out) ◄──────────────────────────────────────────────────────────┘

    Barge-in mechanism:
      - Deepgram interim_results=true gives us partial transcripts the instant
        the partner starts making sound — not just on the final utterance.
      - While the agent is speaking (_speaking=True), any interim OR final
        result arriving sets _interrupt_flag=True.
      - The TTS chunk-sending loop in _send_tts_reply checks this flag before
        every chunk and stops immediately if set.
      - A Twilio 'clear' event is sent to flush any audio already buffered on
        Twilio's side, so playback stops instantly rather than finishing
        whatever was already queued.
      - On the next LLM turn, meta["was_interrupted"] tells the agent to open
        with a brief apology before responding.
    """
    await ws.accept()
    logger.info("Audio stream open SID=%s", call_sid)

    meta = _calls.get(call_sid)
    if not meta:
        logger.warning("Audio WS open for unknown SID=%s — closing", call_sid)
        await ws.close()
        return

    # Initialise conversation history with system prompt.
    partner_name  = meta.get("partner_name", "")
    digitisation  = meta.get("digitisation", "semi")   # default: semi if unknown
    category      = meta.get("category", "")
    company_synopsis = meta.get("company_synopsis", "")
    meta["history"] = [
        {"role": "system", "content": _system_prompt(partner_name, digitisation, category, company_synopsis)},
    ]
    meta["ai_disclosed"] = False
    meta["dnc_requested"] = False
    meta["escalation_requested"] = False
    meta["_call_ended"] = False
    meta["was_interrupted"] = False

    # Call recorder — streams both audio directions to a single WAV file
    # on disk in real time. None if recording is disabled via env var.
    recorder = _CallRecorder(call_sid) if RECORDING_ENABLED else None

    # _speaking: True while we're generating + sending TTS audio.
    _speaking = False
    # _interrupt_flag: set True the instant the partner starts talking while
    # _speaking is True. Checked by the TTS chunk loop to abort mid-stream.
    _interrupt_flag = False
    _last_utterance_ts = asyncio.get_event_loop().time()

    # ── Timing instrumentation ──────────────────────────────────────────
    # Tracks wall-clock (time.monotonic) timestamps at each pipeline stage
    # boundary, so a "gap" reported by the user can be traced to the exact
    # stage that ate the time instead of guessed from message timestamps.
    # _last_audio_recv_ts: updated on every raw audio chunk received from
    # Twilio. If Deepgram emits a final transcript long after this, the
    # gap is on the NETWORK/AUDIO side (weak signal, dropped packets) —
    # not in our pipeline at all.
    _last_audio_recv_ts = time.monotonic()
    # _final_transcript_ts: set the moment Deepgram emits is_final=True.
    # This is t=0 for everything downstream (Groq, TTS, send).
    _final_transcript_ts: float = 0.0

    async def _send_tts_reply(text: str, stream_sid: str) -> bool:
        """
        Synthesise text and stream mulaw audio back to Twilio.
        Returns True if playback completed fully, False if it was cut short
        by a barge-in (caller can check this to decide how to handle the
        next turn).
        """
        nonlocal _speaking, _interrupt_flag
        _interrupt_flag = False
        _speaking = True
        completed = True

        # ── Timing: TTS synthesis stage ──────────────────────────────────
        t_tts_start = time.monotonic()
        try:
            logger.info("[AGENT] %s", text)
            mulaw = await _tts_to_mulaw(text)
            t_tts_done = time.monotonic()
            tts_synth_ms = (t_tts_done - t_tts_start) * 1000

            # If we know when the final transcript landed, log the FULL
            # chain latency: transcript -> TTS audio ready. This isolates
            # whether edge-tts itself is the slow stage.
            if _final_transcript_ts:
                total_to_audio_ready_ms = (t_tts_done - _final_transcript_ts) * 1000
                logger.info(
                    "TIMING SID=%s — TTS synth: %.0fms | transcript→audio_ready: %.0fms",
                    call_sid, tts_synth_ms, total_to_audio_ready_ms,
                )
            else:
                logger.info("TIMING SID=%s — TTS synth: %.0fms", call_sid, tts_synth_ms)

            if mulaw and stream_sid:
                first_chunk_sent = False
                # Send in 160-byte chunks (20ms at 8 kHz mulaw)
                chunk_size = 160 * 10   # 200ms chunks — good balance
                for i in range(0, len(mulaw), chunk_size):
                    # Check for barge-in before EVERY chunk — this is what
                    # makes the cutoff near-instant rather than waiting for
                    # the whole utterance to finish playing.
                    if _interrupt_flag:
                        logger.info(
                            "Barge-in detected mid-speech SID=%s — cutting TTS playback",
                            call_sid,
                        )
                        # Tell Twilio to discard any audio it has buffered
                        # but not yet played, so the cutoff is immediate.
                        try:
                            await ws.send_text(_clear_message(stream_sid))
                        except Exception:
                            pass
                        completed = False
                        break
                    chunk = mulaw[i:i + chunk_size]
                    await ws.send_text(_mulaw_to_twilio_message(chunk, stream_sid))

                    # ── Timing: first chunk actually sent to Twilio ──────
                    # This is the true "partner hears agent start talking"
                    # moment. Logged once per reply, not per chunk.
                    if not first_chunk_sent:
                        first_chunk_sent = True
                        if _final_transcript_ts:
                            total_e2e_ms = (time.monotonic() - _final_transcript_ts) * 1000
                            logger.info(
                                "TIMING SID=%s — END-TO-END (transcript final → first audio sent): %.0fms",
                                call_sid, total_e2e_ms,
                            )

                    # Record agent audio as it's actually sent — keeps the
                    # recording in true chronological sync with what the
                    # partner heard, including any chunk that was cut short
                    # by a barge-in (we only record what was actually played).
                    if recorder is not None:
                        await recorder.write_agent_mulaw(chunk)
                    await asyncio.sleep(0)  # yield to event loop between chunks
        except Exception as exc:
            logger.error("TTS send error SID=%s: %s", call_sid, exc)
        finally:
            _speaking = False
        return completed

    async def _handle_utterance(text: str, speaker: str) -> None:
        """
        Process one STT utterance through intent detection → LLM → TTS pipeline.
        """
        nonlocal _speaking, _interrupt_flag, _last_utterance_ts, _final_transcript_ts
        _last_utterance_ts = asyncio.get_event_loop().time()

        # ── Timing: this is t=0 for the whole reply pipeline ─────────────
        _final_transcript_ts = time.monotonic()
        gap_since_audio_ms = (_final_transcript_ts - _last_audio_recv_ts) * 1000
        logger.info(
            "TIMING SID=%s — final transcript landed %.0fms after last raw audio chunk received "
            "(large value here = network/Deepgram delay, not our pipeline)",
            call_sid, gap_since_audio_ms,
        )

        # CRITICAL: reset the interrupt flag at the START of handling a new
        # utterance, not just inside _send_tts_reply. Without this, if a
        # previous turn was interrupted BEFORE it ever reached _send_tts_reply
        # (e.g. cut off "between sentences" before the first sentence even
        # started playing), the flag stays True forever — every subsequent
        # utterance would see _interrupt_flag already True at first check and
        # abort instantly without ever calling Groq or speaking again. This
        # was the root cause of the agent going permanently silent after the
        # first barge-in for the rest of the call.
        _interrupt_flag = False

        if not text.strip():
            return

        ts = _utc_naive()
        logger.info("[PARTNER] %s: %s", speaker, text)

        meta["transcript"].append({"speaker": speaker, "text": text, "ts": ts.isoformat()})
        await _db_insert_line(call_sid, speaker, text, ts)

        stream_sid = meta.get("stream_sid", "")

        # ── DNC check — hard stop, per Aarna doc ─────────────────────────────
        if _DNC_PATTERNS.search(text):
            meta["dnc_requested"] = True
            logger.info("DNC signal detected SID=%s — offering human agent first", call_sid)
            dnc_reply = (
                "Understood. Would you like me to connect you with a human "
                "from the Aarna team instead, or would you prefer we don't reach out further?"
            )
            meta["history"].append({"role": "user", "content": text})
            meta["history"].append({"role": "assistant", "content": dnc_reply})
            agent_ts = _utc_naive()
            meta["transcript"].append({"speaker": "Agent", "text": dnc_reply, "ts": agent_ts.isoformat()})
            await _db_insert_line(call_sid, "Agent", dnc_reply, agent_ts)
            await _send_tts_reply(dnc_reply, stream_sid)
            return

        # ── If partner confirmed no human agent after DNC — final exit ────────
        if meta.get("dnc_requested"):
            final_reply = "Understood — thanks for letting me know. We won't reach out further. Wishing your business continued success."
            meta["history"].append({"role": "user", "content": text})
            meta["history"].append({"role": "assistant", "content": final_reply})
            agent_ts = _utc_naive()
            meta["transcript"].append({"speaker": "Agent", "text": final_reply, "ts": agent_ts.isoformat()})
            await _db_insert_line(call_sid, "Agent", final_reply, agent_ts)
            await _send_tts_reply(final_reply, stream_sid)
            await asyncio.sleep(2)
            meta["_call_ended"] = True
            try:
                await ws.close()
            except Exception:
                pass
            return

        # ── Escalation check ─────────────────────────────────────────────────
        if _ESCALATION_PATTERNS.search(text):
            meta["escalation_requested"] = True
            logger.info("Escalation trigger detected SID=%s", call_sid)
            escalation_reply = (
                "That's a great question for our partnership lead — I'll "
                "make sure they cover that on your call. Have a wonderful day!"
            )
            meta["history"].append({"role": "user", "content": text})
            meta["history"].append({"role": "assistant", "content": escalation_reply})
            agent_ts = _utc_naive()
            meta["transcript"].append({"speaker": "Agent", "text": escalation_reply, "ts": agent_ts.isoformat()})
            await _db_insert_line(call_sid, "Agent", escalation_reply, agent_ts)
            await _send_tts_reply(escalation_reply, stream_sid)
            await asyncio.sleep(2)
            meta["_call_ended"] = True
            try:
                await ws.close()
            except Exception:
                pass
            return

        # ── Normal LLM flow — SENTENCE-STREAMED ─────────────────────────────
        # This is the core "no void" fix. Instead of:
        #   wait for full Groq reply → TTS the whole thing → send
        # we now do, per sentence:
        #   Groq emits sentence 1 → TTS sentence 1 → send IMMEDIATELY
        #   (while Groq is still generating sentence 2 in the background)
        # The partner hears the agent start speaking within ~400-700ms of
        # finishing their utterance, instead of 2-3+ seconds.
        if meta.get("was_interrupted"):
            meta["history"].append({
                "role": "system",
                "content": (
                    "You were just interrupted mid-sentence by the supplier. "
                    "Open your next reply with a brief apology (3-5 words max), "
                    "then respond directly to what they just said. Do not repeat "
                    "or finish your previous thought."
                ),
            })
            meta["was_interrupted"] = False

        meta["history"].append({"role": "user", "content": text})

        full_reply_parts: list[str] = []
        any_sentence_completed = True
        was_cut_short = False
        _sentence_index = 0

        async for sentence in _llm_reply_stream(meta["history"]):
            _sentence_index += 1
            # ── Timing: Groq first-sentence latency ───────────────────────
            # Logged only for the first sentence of each reply — this isolates
            # how long Groq took to generate the first usable chunk of text,
            # independent of how many sentences the full reply has.
            if _sentence_index == 1:
                groq_first_sentence_ms = (time.monotonic() - _final_transcript_ts) * 1000
                logger.info(
                    "TIMING SID=%s — Groq first sentence ready: %.0fms after transcript",
                    call_sid, groq_first_sentence_ms,
                )

            # Check barge-in BEFORE starting this sentence's TTS — if the
            # partner started talking while the previous sentence was
            # playing, stop generating/speaking further sentences entirely.
            if _interrupt_flag:
                logger.info(
                    "Barge-in detected between sentences SID=%s — stopping reply generation",
                    call_sid,
                )
                was_cut_short = True
                break

            full_reply_parts.append(sentence)

            agent_ts = _utc_naive()
            meta["transcript"].append({"speaker": "Agent", "text": sentence, "ts": agent_ts.isoformat()})
            await _db_insert_line(call_sid, "Agent", sentence, agent_ts)

            completed = await _send_tts_reply(sentence, stream_sid)
            any_sentence_completed = any_sentence_completed and completed

            if not completed:
                # Barge-in occurred mid-sentence — don't continue to the
                # next sentence even if Groq already generated it.
                was_cut_short = True
                break

        full_reply = " ".join(full_reply_parts).strip()
        if full_reply:
            meta["history"].append({"role": "assistant", "content": full_reply})

        if was_cut_short:
            logger.info("Reply to SID=%s was interrupted before completion", call_sid)

        # Detect call-ending intent — only check if nothing was cut off,
        # and check across the FULL reply (farewell phrase might span
        # what we split into separate sentences).
        if (not was_cut_short and not meta.get("_call_ended")
                and full_reply and _FAREWELL_PATTERNS.search(full_reply)):
            logger.info("Agent said farewell — closing stream SID=%s", call_sid)
            meta["_call_ended"] = True
            await asyncio.sleep(2)   # let TTS finish playing before closing
            try:
                await ws.close()
            except Exception:
                pass

    async def _silence_nudge_loop() -> None:
        """
        Plays a single "Are you still there?" nudge ONLY if the partner has
        said absolutely nothing since the call connected — i.e. the opening
        line played and then there was total silence. This is for genuinely
        dead calls (bad line, partner walked away).

        Once the partner has spoken even once, this loop does nothing for
        the rest of the call — natural conversational pauses (thinking,
        checking something) must NEVER trigger an unsolicited nudge or
        script recitation. That was the root cause of the agent appearing
        to "restart the script" mid-conversation.
        """
        nonlocal _last_utterance_ts
        await asyncio.sleep(SILENCE_NUDGE_S)

        # Only fire if the partner has NEVER spoken — zero entries in
        # transcript from anyone other than the system/agent's own opening.
        partner_has_spoken = any(
            e["speaker"] not in ("Agent", "system") for e in meta.get("transcript", [])
        )
        if partner_has_spoken:
            logger.debug(
                "Silence nudge skipped SID=%s — partner has already spoken, "
                "this is a natural pause not a dead call.",
                call_sid,
            )
            return

        if _speaking or _interrupt_flag:
            return

        logger.info("Silence nudge firing SID=%s — zero exchange after %ds", call_sid, SILENCE_NUDGE_S)
        nudge = "Hello? Are you there?"
        meta["history"].append({"role": "assistant", "content": nudge})
        stream_sid = meta.get("stream_sid", "")
        await _send_tts_reply(nudge, stream_sid)
        _last_utterance_ts = asyncio.get_event_loop().time()
        # Nudge fires once only, ever — no loop, no repeat. If still silent
        # after this, Twilio's own call timeout will eventually end it.

    # -----------------------------------------------------------------------
    # Connect to Deepgram
    #
    # ARCHITECTURE FIX — this is the core bug fix for "doesn't pause when
    # client speaks, 8-second response gaps". Previously, ONE task did both
    # (a) continuously read Deepgram messages AND (b) run the entire slow
    # LLM → TTS → Twilio pipeline inline, awaited in sequence. This meant
    # that while we were inside _send_tts_reply's chunk-sending loop, we
    # were NOT reading new messages off the Deepgram WebSocket at all —
    # the receive loop was paused deep in the call stack. Even though
    # Deepgram kept transcribing the partner's mic on its own server and
    # tried to push us interim results, we simply weren't listening. The
    # barge-in flag could only ever be checked once we returned to the
    # Deepgram read loop, which only happened after the ENTIRE reply
    # finished playing — making barge-in a no-op in practice and adding
    # multi-second stalls whenever Groq/TTS were slow.
    #
    # FIX: split into three genuinely independent concurrent tasks:
    #   1. recv_from_twilio    — forwards partner audio to Deepgram (fast, never blocks)
    #   2. recv_from_deepgram  — ONLY reads Deepgram messages, sets the barge-in
    #                            flag instantly, and pushes final utterances onto
    #                            an asyncio.Queue. NEVER calls the LLM/TTS pipeline
    #                            directly, so it can never be blocked by them.
    #   3. process_utterances  — drains the queue and runs the slow LLM → TTS
    #                            pipeline, completely independently. Because this
    #                            is now a SEPARATE task, task 2 keeps reading
    #                            Deepgram in real time the whole time task 3 is
    #                            busy speaking — so barge-in detection now has
    #                            zero delay regardless of how slow a reply is.
    # -----------------------------------------------------------------------
    try:
        async with websockets.connect(
            DEEPGRAM_URL,
            # websockets >= 12 requires additional_headers as list of tuples,
            # NOT a dict — passing a dict causes HTTP 400 immediately.
            additional_headers=[("Authorization", f"Token {DEEPGRAM_API_KEY}")],
            ping_interval=20,
            ping_timeout=10,
        ) as dg_ws:

            # Queue of (text, speaker) tuples — final utterances waiting to
            # be processed through the LLM/TTS pipeline. maxsize=1 means if
            # the partner says multiple things while the agent is mid-reply,
            # only the MOST RECENT is kept (we discard stale ones rather than
            # queuing a backlog — the partner doesn't want answers to things
            # they said three turns ago).
            utterance_queue: asyncio.Queue = asyncio.Queue(maxsize=1)

            # ----------------------------------------------------------------
            # Task 1: Twilio → Deepgram (forward raw mulaw audio)
            # ----------------------------------------------------------------
            async def recv_from_twilio():
                nonlocal _last_audio_recv_ts
                async for message in ws.iter_text():
                    data = json.loads(message)
                    event = data.get("event")

                    if event == "start":
                        meta["stream_sid"] = data.get("start", {}).get("streamSid", "")
                        logger.info("Stream started SID=%s StreamSid=%s",
                                    call_sid, meta["stream_sid"])

                    elif event == "media":
                        raw_audio = base64.b64decode(data["media"]["payload"])
                        # Timing: stamp every raw audio chunk arrival. Used to
                        # detect if audio itself is arriving with gaps (weak
                        # mobile signal, Twilio buffering) before it ever
                        # reaches Deepgram — distinguishes network delay from
                        # pipeline delay.
                        _last_audio_recv_ts = time.monotonic()
                        await dg_ws.send(raw_audio)
                        if recorder is not None:
                            await recorder.write_partner_mulaw(raw_audio)

                    elif event == "stop":
                        logger.info("Twilio stream stopped SID=%s", call_sid)
                        try:
                            await dg_ws.close()
                        except Exception:
                            pass
                        break

            # ----------------------------------------------------------------
            # Task 2: Deepgram receiver — ONLY job is reading messages and
            # setting flags / queueing utterances. NEVER calls the LLM or
            # TTS pipeline directly. This is what keeps it always responsive.
            # ----------------------------------------------------------------
            async def recv_from_deepgram():
                nonlocal _interrupt_flag
                async for msg in dg_ws:
                    result = json.loads(msg)

                    if result.get("type") != "Results":
                        continue

                    alts = result.get("channel", {}).get("alternatives", [])
                    if not alts:
                        continue

                    text = alts[0].get("transcript", "").strip()
                    if not text:
                        continue

                    is_final = result.get("is_final", False)

                    # ── Barge-in detection — now genuinely instant ──────────
                    # This check fires the moment ANY Deepgram message arrives,
                    # regardless of what task 3 is currently doing, because
                    # this task is never blocked waiting on Groq/TTS/Twilio.
                    if _speaking and not _interrupt_flag:
                        logger.info(
                            "Barge-in signal SID=%s — partner speaking: %r",
                            call_sid, text[:40],
                        )
                        _interrupt_flag = True
                        meta["was_interrupted"] = True

                    if not is_final:
                        continue

                    words   = alts[0].get("words", [])
                    speaker = f"Speaker {words[0].get('speaker', 0)}" if words else "Speaker 0"

                    # Push onto the queue instead of processing inline.
                    # If a previous utterance is still queued (pipeline task
                    # hasn't picked it up yet), drop it in favour of this
                    # newer one — never build up a stale backlog.
                    if utterance_queue.full():
                        try:
                            utterance_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    await utterance_queue.put((text, speaker))

            # ----------------------------------------------------------------
            # Task 3: Pipeline processor — drains the queue, runs the slow
            # LLM → TTS → Twilio pipeline. Runs independently of task 2, so
            # task 2 keeps listening to Deepgram the entire time this task
            # is busy speaking. THIS is what makes barge-in actually work.
            # ----------------------------------------------------------------
            async def process_utterances():
                while True:
                    text, speaker = await utterance_queue.get()
                    try:
                        await _handle_utterance(text, speaker)
                    except Exception as exc:
                        logger.error(
                            "Error processing utterance SID=%s: %s", call_sid, exc
                        )
                    finally:
                        utterance_queue.task_done()

            # ----------------------------------------------------------------
            # Run all tasks concurrently
            # ----------------------------------------------------------------
            nudge_task = asyncio.create_task(_silence_nudge_loop())
            pipeline_task = asyncio.create_task(process_utterances())
            try:
                await asyncio.gather(recv_from_twilio(), recv_from_deepgram())
            finally:
                nudge_task.cancel()
                pipeline_task.cancel()
                for t in (nudge_task, pipeline_task):
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

    except Exception as exc:
        logger.warning("Audio stream error SID=%s: %s", call_sid, exc)

    logger.info("Audio stream closed SID=%s", call_sid)

    # Finalise the recording — flushes any buffered audio and closes the WAV.
    if recorder is not None:
        recording_path = await recorder.close()
        if recording_path:
            meta["recording_path"] = recording_path
            logger.info("Call recording saved SID=%s path=%s", call_sid, recording_path)
        else:
            logger.info("Call recording skipped SID=%s — no audio captured", call_sid)

    # Small buffer — let any trailing Deepgram results land
    await asyncio.sleep(1)

    if call_sid in _calls:
        await _post_process(call_sid)


# ---------------------------------------------------------------------------
# Post-call: summarise full transcript → DB → signal done_event
# ---------------------------------------------------------------------------

async def _post_process(call_sid: str) -> None:
    meta       = _calls.get(call_sid, {})
    transcript = meta.get("transcript", [])

    logger.info("Post-processing SID=%s (%d turns)", call_sid, len(transcript))

    readable = "\n".join(
        f"[{e['ts']}] {e['speaker']}: {e['text']}" for e in transcript
    ) or "(no transcript — call not answered or too short)"

    parsed, raw = await _summarise_with_groq(
        transcript=readable,
        mission=meta.get("mission", "Outbound partner call"),
        to=meta.get("to", ""),
        duration=str(meta.get("duration", 0)),
    )

    await _db_insert_summary(call_sid, parsed, raw)

    # ── Calendar invite — sent ONLY if a specific date+time was actually
    # agreed during the call (see meeting_scheduled/meeting_date/
    # meeting_time in the Groq summary schema above). Never triggered by
    # sentiment alone — a positive call with no concrete time agreed
    # must not generate a speculative invite.
    from voice_agent.calendar_invite import maybe_send_calendar_invite
    partner_email = meta.get("partner_email") or meta.get("email_id") or ""
    invite_sent = await maybe_send_calendar_invite(
        partner_name=meta.get("partner_name", ""),
        partner_email=partner_email,
        summary=parsed,
        call_sid=call_sid,
    )
    if invite_sent:
        logger.info("Calendar invite sent SID=%s to %s", call_sid, partner_email)
    # ─────────────────────────────────────────────────────────────────────

    # Persist the recording path to outreach_calls so it's queryable
    # alongside the rest of the call record.
    if meta.get("recording_path"):
        await _db_upsert_call(call_sid, recording_path=meta["recording_path"])

    meta["result"] = {
        "call_sid":       call_sid,
        "status":         meta.get("status", "completed"),
        "duration_s":     meta.get("duration", 0),
        "summary":        parsed,
        "recording_path": meta.get("recording_path"),
    }
    meta["done_event"].set()
    logger.info("Summary stored SID=%s outcome=%r", call_sid, parsed.get("outcome"))


async def _summarise_with_groq(
    transcript: str, mission: str, to: str, duration: str
) -> tuple[dict, str]:
    # Today's date in Dubai time — gives the LLM context to resolve
    # relative dates the partner may have used ("next month", "the 22nd")
    # into an absolute, unambiguous calendar date rather than guessing.
    today_dubai = (datetime.now(timezone.utc) + timedelta(hours=4)).strftime("%Y-%m-%d (%A)")

    prompt = f"""Analyse this outbound business call transcript.

Call metadata:
- Mission: {mission}
- Called: {to}
- Duration: {duration}s
- Today's date (Dubai time): {today_dubai}

Transcript:
{transcript}

Respond ONLY with a valid JSON object — no markdown, no preamble:
{{
  "outcome": "1-sentence outcome",
  "key_points": ["point 1", "point 2"],
  "action_items": ["action 1", "action 2"],
  "sentiment": "Positive | Neutral | Negative — one-line reason",
  "notable_quotes": ["quote 1"],
  "meeting_scheduled": true or false — true ONLY if the partner explicitly
    agreed to a specific follow-up call/meeting with a real date AND time,
    not just general interest or "maybe later",
  "meeting_date": "YYYY-MM-DD" or null — the agreed date, resolved to an
    absolute date using today's date above (e.g. if they said "next
    Tuesday" or "the 22nd", work out the actual calendar date). Use null
    if no specific date was agreed or the transcript is ambiguous —
    never guess.,
  "meeting_time": "HH:MM" or null — the agreed time in 24-hour format,
    Dubai local time. Use null if no specific time was agreed — never
    guess or default to a made-up time.
}}

If call was not answered or transcript is empty, set outcome accordingly and leave lists empty."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "max_tokens": 600,
                },
            )
            r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.error("Groq summary failed: %s", exc)
        return {
            "outcome": "Summary unavailable (Groq error)",
            "key_points": [], "action_items": [], "sentiment": "Unknown", "notable_quotes": [],
            "meeting_scheduled": False, "meeting_date": None, "meeting_time": None,
        }, ""

    try:
        parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
    except json.JSONDecodeError:
        parsed = {"outcome": raw, "key_points": [], "action_items": [],
                  "sentiment": "Unknown", "notable_quotes": [],
                  "meeting_scheduled": False, "meeting_date": None, "meeting_time": None}

    # Defensive defaults — if the LLM omits these keys entirely (older
    # response shape, or a malformed JSON that still parsed), never let
    # a missing key silently pass a truthy check downstream.
    parsed.setdefault("meeting_scheduled", False)
    parsed.setdefault("meeting_date", None)
    parsed.setdefault("meeting_time", None)

    return parsed, raw


# ---------------------------------------------------------------------------
# DB helpers — thin wrappers over shared asyncpg pool
# ---------------------------------------------------------------------------

def _utc_naive() -> "datetime":
    """
    Return current UTC time as a timezone-NAIVE datetime.
    Supabase uses pgbouncer in transaction mode. asyncpg maps Python's
    timezone-aware datetimes to TIMESTAMPTZ, but our schema uses TIMESTAMP
    (no timezone). Stripping tzinfo (while keeping the UTC value) fixes this.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _db_upsert_call(call_sid: str, **kwargs) -> None:
    pool = await get_pool()
    cols   = list(kwargs.keys())
    values = list(kwargs.values())

    col_list     = ", ".join(["call_sid"] + cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(values) + 1))
    set_clause   = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(cols))

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO outreach_calls ({col_list})
                VALUES ({placeholders})
                ON CONFLICT (call_sid) DO UPDATE SET {set_clause}
                """,
                call_sid, *values,
            )
    except Exception as exc:
        logger.warning("DB upsert failed SID=%s: %s", call_sid, exc)


async def _db_insert_line(call_sid: str, speaker: str, text: str, ts: datetime) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO outreach_transcript_lines (call_sid, speaker, line_text, spoken_at) "
                "VALUES ($1, $2, $3, $4)",
                call_sid, speaker, text, ts,
            )
    except Exception as exc:
        logger.warning("DB line insert failed SID=%s: %s", call_sid, exc)


async def _db_insert_summary(call_sid: str, parsed: dict, raw: str) -> None:
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO outreach_call_summaries
                    (call_sid, outcome, key_points, action_items, sentiment, notable_quotes, raw_summary)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (call_sid) DO UPDATE SET
                    outcome        = EXCLUDED.outcome,
                    key_points     = EXCLUDED.key_points,
                    action_items   = EXCLUDED.action_items,
                    sentiment      = EXCLUDED.sentiment,
                    notable_quotes = EXCLUDED.notable_quotes,
                    raw_summary    = EXCLUDED.raw_summary
                """,
                call_sid,
                parsed.get("outcome", ""),
                json.dumps(parsed.get("key_points", [])),
                json.dumps(parsed.get("action_items", [])),
                parsed.get("sentiment", ""),
                json.dumps(parsed.get("notable_quotes", [])),
                raw,
            )
    except Exception as exc:
        logger.warning("DB summary insert failed SID=%s: %s", call_sid, exc)


# ---------------------------------------------------------------------------
# Call history API
# ---------------------------------------------------------------------------

@router.get("/api/calls")
async def list_calls(limit: int = 50):
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT c.call_sid, c.partner_name, c.to_number, c.status,
                       c.duration_s, c.started_at, c.ended_at, c.recording_path,
                       s.outcome, s.sentiment
                FROM outreach_calls c
                LEFT JOIN outreach_call_summaries s ON s.call_sid = c.call_sid
                ORDER BY c.created_at DESC
                LIMIT $1
                """,
                limit,
            )
        return {"calls": [dict(r) for r in rows]}
    except Exception as exc:
        return {"calls": [], "error": str(exc)}


@router.get("/api/calls/{call_sid}")
async def get_call_detail(call_sid: str):
    import json as _json
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            call = await conn.fetchrow("SELECT * FROM outreach_calls WHERE call_sid = $1", call_sid)
            if not call:
                return {"error": "Call not found"}

            lines = await conn.fetch(
                "SELECT speaker, line_text, spoken_at FROM outreach_transcript_lines "
                "WHERE call_sid = $1 ORDER BY id",
                call_sid,
            )
            summary = await conn.fetchrow(
                "SELECT * FROM outreach_call_summaries WHERE call_sid = $1", call_sid
            )

        return {
            "call":       dict(call),
            "transcript": [dict(l) for l in lines],
            "summary": {
                "outcome":        summary["outcome"]                                          if summary else None,
                "sentiment":      summary["sentiment"]                                        if summary else None,
                "key_points":     _json.loads(summary["key_points"]     or "[]")             if summary else [],
                "action_items":   _json.loads(summary["action_items"]   or "[]")             if summary else [],
                "notable_quotes": _json.loads(summary["notable_quotes"] or "[]")             if summary else [],
                "raw":            summary["raw_summary"]                                      if summary else None,
            } if summary else None,
        }
    except Exception as exc:
        return {"error": str(exc)}