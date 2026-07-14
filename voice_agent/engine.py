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

# Speech rate — sped up from edge-tts's default pace per explicit latency
# complaint. This is a purely mechanical fix: it shortens every reply's
# real playback duration proportionally without changing a single word
# of the script. A long multi-sentence turn (e.g. the STEP 4 pitch) can
# easily run 18-20+ real seconds at normal pace once played back in real
# time — most of what was being perceived as "latency"/"the bot won't
# stop talking" is actually just how long that much text takes to say
# out loud, not network or API delay (Groq/TTS themselves respond in
# under 1-1.5s per sentence throughout these logs).
TTS_RATE = os.getenv("TTS_RATE", "+15%")

# Groq model — llama-3.3-70b: best quality/cost ratio on Groq
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# ---------------------------------------------------------------------------
# Deepgram params — Nova-2 English, endpointing at 700ms silence.
#
# Reduced from 1100ms per explicit latency complaint — this delay happens
# BEFORE _final_transcript_ts is even set, so it was invisible in all the
# TIMING log lines (which only measure from is_final onward) while still
# being real dead air the caller experienced on every single turn. 700ms
# is a reasonable floor — going much lower risks Deepgram treating a
# natural mid-sentence breath/pause as the end of the utterance and
# cutting the caller off prematurely. If callers start getting cut off
# mid-sentence, raise this back up; that's the direct tradeoff being made.
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
    "&endpointing=700"
    "&vad_events=true"
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

# Category-specific personalized pitch questions — used in STEP 4 instead
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


def _personalized_pitch_question(partner_name: str, category: str, company_synopsis: str) -> str:
    """
    Build the "reason for calling" + OTA-question line for STEP 4.

    Merged down to exactly 2 sentences (one statement + the question),
    per the hard cap on sentences-per-turn. Confirmed real failure this
    fixes: the model was faithfully speaking this content as 3-4 separate
    short sentences back-to-back with no pause (since that's how many
    sentences the old template itself was punctuated as, and the model
    was told to preserve "exact meaning" — which it did, including the
    sentence count). Same meaning and specifics as before, just phrased
    as fewer, fuller sentences joined with a dash/conjunction instead of
    splitting every clause into its own sentence. The question is always
    its own final sentence — never merged into the statement above it.

    category drives the {category} slot naturally (e.g. "Activities" ->
    "activities marketplace", "Wellness" -> "wellness marketplace").
    Falls back to a generic "experiences" wording if category is unknown.
    Never invents details not actually given.
    """
    partner = partner_name or "your business"

    if category:
        cat_word = category.strip().lower()
    else:
        cat_word = "experiences"

    if company_synopsis:
        return (
            f"I looked into {partner} — {company_synopsis} — and we're "
            f"currently onboarding suppliers like you for our new {cat_word} "
            f"marketplace, Aarna. Are you currently working with any online "
            f"travel platforms or marketplaces?"
        )

    return (
        f"We're currently onboarding suppliers for our new {cat_word} "
        f"marketplace, Aarna, and came across {partner} as a great fit. "
        f"Are you currently working with any online travel platforms or "
        f"marketplaces?"
    )


def _system_prompt(
    partner_name: str,
    digitisation: str,
    category: str = "",
    company_synopsis: str = "",
) -> str:
    """
    Build the system prompt for the live call.

    Exact required script — separate turns for identity confirmation,
    good-time check, then reason-for-calling (with category-specific
    marketplace wording), then scheduling. New flow:

        (opening now spoken via our own pipeline over the WebSocket,
         not Twilio's <Say> — fixes the recording missing the opening) ->
        STEP 1: identity confirmed ->
        STEP 2: "is it a good time?" (its own turn) ->
        STEP 3: branch on good-time answer; if yes, go straight into
                STEP 4 in the same turn (no separate AI-disclosure turn
                anymore — flagged explicitly per the latest exact script;
                previously this WAS its own turn, then was removed, then
                re-added — if AI disclosure is still needed for compliance
                in some regions, it should come back as an inline clause
                within this same turn rather than its own turn) ->
        STEP 4: reason for calling + OTA question (exact wording,
                category-driven) ->
        STEP 5: offer + schedule a human-agent call ->
        STEP 6: confirm + close

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
      drives the personalized STEP 4 pitch question. Optional.
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

CRITICAL — AI DISCLOSURE:
You have already identified yourself as an AI in the opening line
("I am Sania, an AI assistant from Aarna"). If at any point the
caller asks whether they are speaking to a human or a bot, confirm
honestly and briefly: "Yes, I am an AI assistant — Sania from
Aarna." Never deny being an AI.


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

CRITICAL — NEVER REPEAT CONTENT YOU HAVE ALREADY SAID:
Before every reply, check the conversation history above. If you have
ALREADY said a point earlier in this same call, do NOT say it again —
even after an interruption, a confusing or unclear response from the
supplier, or a detour into answering a question. Confirmed real failures
this causes if not followed:
- After being interrupted mid-way through STEP 4, the agent restarted
  the ENTIRE STEP 4 pitch from the beginning, saying the exact same
  sentences twice.
- After answering "what is Aarna" mid-way through STEP 5, the agent
  re-asked the exact same STEP 5 scheduling question it had already
  asked minutes earlier, verbatim — and did this a THIRD time later in
  the same call.
- The supplier was mid-way through hearing the "what is Aarna" answer
  and said something encouraging like "Yeah, I would love to [hear
  more]" WHILE the agent was still talking. This is normal backchannel
  encouragement to keep going, not a request to stop or change subject
  — but it still triggers the interruption mechanism (any real
  transcribed speech can cut the agent off, by design, since telling a
  genuine interruption apart from encouragement isn't reliable). The
  agent's WRONG response was to abandon the explanation entirely and
  jump back to the STEP 5 scheduling question. Later, asked again, it
  then repeated the exact same first sentence of the explanation from
  scratch instead of continuing or building on it.
All of these are repetition/abandonment failures, not correct behaviour.
Instead:
- If interrupted mid-explanation and the supplier's response sounds like
  encouragement or agreement to keep listening ("yeah", "I'd love to",
  "please continue", "tell me more", or similar short affirmations, NOT
  a new question or objection) — CONTINUE the explanation from roughly
  where you left off, in your own words. Do not restart it, do not
  abandon it for an unrelated next step, and do not repeat a sentence
  you already said — say the REMAINING point you hadn't gotten to yet.
- If interrupted mid-pitch and the supplier's response doesn't clearly
  answer or continue anything (garbled, nonsensical, unrelated), do NOT
  restart the pitch from the top. Ask ONE short clarifying question
  instead — "Sorry, could you say that again?" — and wait.
- If you already asked a specific question earlier in the call (e.g. the
  STEP 5 scheduling question) and a detour happened since (like
  answering "what is Aarna"), do NOT re-ask it with the identical
  wording. Briefly bridge back instead — e.g. "So, back to what I was
  asking — would tomorrow work?" — shorter than the original, not a
  repeat of it.
- When genuinely unsure whether something was already said, default to
  a SHORTER, differently-worded next step rather than repeating a full
  sentence you may have already used.

CRITICAL — NEVER HALLUCINATE:
If the supplier asks something you don't have a clear, explicit answer for
in this prompt (exact pricing, commission percentages, contract terms,
specific numbers, launch dates, or anything else not stated here), do NOT
invent an answer or guess. Say something like: "That's a great question —
I'll make sure our partnership lead covers that on your call." Then move
on. Never fill a gap with a plausible-sounding but made-up detail.

This deferral phrase is ONLY for genuine unanswered questions about
SPECIFIC facts you were not given (exact numbers, pricing, contract
terms). It must NEVER be used in reply to a plain scheduling statement —
a date, a time, "yes that works", "tomorrow at 2", or similar. Those get
a short, direct confirmation ONLY (e.g. "Perfect, 2PM tomorrow works —
I'll send the invite."), with no deferral phrase attached, even if you
used the deferral phrase earlier in this same call for a different,
unrelated question. Each reply must be judged only on what was JUST
said, not on carrying over a pattern from a previous turn.

The deferral phrase is ALSO the wrong response in these common cases —
confirmed real failures where it was misused, do not repeat them:
- An INCOMPLETE sentence that trails off ("before we set up a meeting, I
  want—") is NOT a question at all. Do not deploy the deferral phrase on
  a fragment. Instead say something like "Sorry, go ahead" or "You were
  saying?" and wait for them to finish.
- "What is this call about?" / "What's the purpose of this call?" IS
  answerable — you already have the purpose statement from STEP 4. Give
  it directly, no deferral needed.
- NEVER use this exact phrase more than twice in one call. If a third
  similar question comes up, do not repeat the same sentence again —
  either answer with what you actually do know, or simply say "I don't
  have that detail handy, but I'll flag it for the call" using different
  wording, so the call doesn't sound like a broken record.

WHAT IS AARNA — exact answer, use whenever asked "what is Aarna",
"tell me about Aarna", "what does Aarna do", "how does this work", "how
will this help me", or anything else asking what Aarna actually is or
does. This is a fully answerable question — always give this exact
answer, never defer it. Merged to 2 sentences (was previously 4 short
ones spoken back-to-back with no pause — same fix as STEP 4 below):
"Aarna is a travel management platform — rather than just relying on
customers visiting our website, we also distribute experiences through
Mondee's global network of over 65,000 travel advisors, plus corporate
and enterprise channels. That gives suppliers like you more ways to
reach new customers."

═══════════════════════════════════════════════════════════════
CALL FLOW — EXACT SCRIPT. FOLLOW STEP BY STEP, ONE TURN EACH.
═══════════════════════════════════════════════════════════════
The opening line has ALREADY been spoken before you say anything (now
sent through your own voice, not a separate system):
"Hi, I am Sania, an AI assistant from Aarna - a travel management platform.
Are you [name] from [company]?"

If there was no response at all, a short "Hello? Are you there?" check-in
has ALSO already played automatically — you don't need to say this
yourself, it happens before you get involved.

Your job starts with THEIR response to the opening. Follow this flow
EXACTLY, one step at a time. WAIT for each response to genuinely finish
before you reply — never jump in on a pause, never respond to a partial
thought. Every step is ONE short sentence or two at most.

STEP 1 — They confirm their identity (a name, "yes", "speaking", etc.).
If unclear or they ask you to repeat, briefly repeat the opening once;
if still unclear a second time, politely end the call. Otherwise, once
confirmed, move immediately to STEP 2 — do not add anything else in
this turn.

STEP 2 — Ask directly: "Is it a good time to talk?" Nothing else in this
turn. Wait for their answer in full before continuing.

STEP 3 — Branch on their answer to the good-time check.
- If NOT a good time (busy, later, no): do not push forward. Offer a
  calendar invite instead: "No problem — would it help if I sent a
  calendar invite so we can find a better time?" Capture a preferred
  day/time if they give one, otherwise just confirm you'll send
  something by email, then close warmly.
- If it IS a good time: go DIRECTLY into STEP 4 in this SAME turn — do
  not stop and wait for a separate response in between. There is no
  standalone disclosure turn anymore; move straight to the reason for
  calling below.

STEP 4 — Say this exactly, adapted naturally in your own words but
keeping its exact meaning and specifics — do not revert to a generic
question about "your experiences" if a personalized one is given below:

"{pitch_question}"

Wait for their full answer before responding. Listen for what they
actually said, but the DEFAULT next step is STEP 5 (arrange the human
call) regardless of whether they're already on other platforms — being
on other platforms is not a reason to stop, it's just useful context.
Only deviate from moving to STEP 5 in these specific cases:
- If they ask what Aarna is (very likely here, e.g. "I'd love to know
  what Aarna is"): give the exact "WHAT IS AARNA" answer from above,
  THEN continue to STEP 5 in the same or next turn — do not skip
  scheduling just because they asked a question first.
- If they sound clearly NOT INTERESTED (dismissive, "not right now",
  "we're fine", explicitly declining): do not push or re-pitch.
  Acknowledge politely and offer once to send information by email
  instead of continuing to press for a call. If they decline that too,
  close the call warmly without scheduling anything.
- If what they describe is CLEARLY unrelated to UAE experiences/
  activities (software, retail, unrelated professional services,
  manufacturing, etc.), this is a fit problem — say something like:
  "Ah, that's not quite the right fit for Aarna, which focuses on UAE
  experience and activity providers — apologies for the mix-up, and
  thanks for your time! Have a wonderful day!" Then end the call. Ask
  one clarifying question first if genuinely unsure rather than
  guessing either way.

STEP 5 — Offer and schedule the human-agent call. Say something like:
"I can arrange for a human partnerships team member to reach out to
you — would you be available for a call tomorrow, or would you prefer
a different time?" Once they agree, capture their preferred day/time
clearly and repeat it back to confirm you heard it right. When they
state a date/time, this is a plain scheduling answer, not a question —
respond with a direct confirmation only, never the deferral phrase from
the anti-hallucination rule above.

STEP 6 — Confirm and close. Say something like: "Perfect, I'll send a
calendar invite to your email. Have a wonderful day!"

TONE:
- Short. Crisp. To the point. HARD CAP: never more than 2 sentences in a
  single turn, no exceptions — including for the STEP 4 pitch and the
  WHAT IS AARNA answer below, both already written as 2 sentences for
  exactly this reason. If you're ever tempted to add a 3rd sentence,
  merge it into one of the first two with a dash or "and" instead of
  speaking it separately.
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
    communicate = edge_tts.Communicate(text, voice=TTS_VOICE, rate=TTS_RATE)
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
    Writes just top up whichever side's buffer. A BACKGROUND TASK running
    on a real 20ms wall-clock schedule (see _periodic_flush_loop) drains
    exactly one frame per real tick — zero-filling whichever side has
    nothing queued at that instant — and writes it to disk.

    FIX (2026-07-12): this used to flush REACTIVELY — every write() call
    immediately drained however many complete frames were queued. That
    was fine for partner audio (Twilio delivers it in real time anyway),
    but agent (TTS) audio is dispatched to Twilio in a fast burst (see
    _send_tts_reply) — an entire sentence's audio can land in this
    recorder's buffer within milliseconds. Reactive flushing wrote all of
    it to the WAV file immediately, advancing the recording's timeline by
    however many seconds that sentence represents — INSTANTLY, in
    wall-clock terms. But real time hadn't actually elapsed yet: Twilio
    kept streaming the caller's real mic audio for that same span as it
    actually happened, which got written as MORE frames once it arrived,
    since the agent buffer was already empty by then. The same real
    interval ended up represented twice — confirmed real case: a 112s
    call produced a 160s recording, a ~48s inflation matching roughly how
    long the agent spent talking that call. Writing frames on a genuine,
    drift-corrected wall-clock schedule instead of reactively fixes this:
    a burst of agent audio just sits in the buffer and gets drained at
    the correct natural pace, exactly like partner audio already was.
    """

    # 20ms at 8kHz = 160 samples — matches Twilio's native frame size.
    _FRAME_SAMPLES = 160
    _FRAME_DURATION_S = _FRAME_SAMPLES / 8000.0   # 0.02s

    def __init__(self, call_sid: str):
        self.call_sid = call_sid
        self.path = _RECORDINGS_DIR / f"{call_sid}.wav"
        self._wav: "wave.Wave_write | None" = None
        self._partner_buf = bytearray()   # PCM16 bytes, partner side
        self._agent_buf   = bytearray()   # PCM16 bytes, agent side
        self._frames_written = 0
        self._lock = asyncio.Lock()
        self._flush_task: "asyncio.Task | None" = None

    def _ensure_open(self) -> None:
        if self._wav is None:
            self._wav = wave.open(str(self.path), "wb")
            self._wav.setnchannels(1)
            self._wav.setsampwidth(2)   # 16-bit PCM
            self._wav.setframerate(8000)

    def start(self) -> None:
        """Begin the real-time periodic flush loop. Call once per recorder."""
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush_loop())

    async def _periodic_flush_loop(self) -> None:
        """
        Writes exactly one mixed frame per real 20ms tick, for the life of
        the call. Uses an absolute schedule (start + tick*frame_duration)
        rather than repeated sleep(0.02) calls, so small scheduling
        overhead each iteration can't accumulate into meaningful drift
        over a multi-minute call.
        """
        start = time.monotonic()
        tick = 0
        try:
            while True:
                tick += 1
                target = start + tick * self._FRAME_DURATION_S
                now = time.monotonic()
                if target > now:
                    await asyncio.sleep(target - now)
                async with self._lock:
                    self._write_one_frame()
        except asyncio.CancelledError:
            pass

    def _write_one_frame(self) -> None:
        """Write exactly one mixed frame from whatever is currently queued."""
        frame_bytes = self._FRAME_SAMPLES * 2  # 2 bytes per PCM16 sample

        p_chunk = bytes(self._partner_buf[:frame_bytes])
        a_chunk = bytes(self._agent_buf[:frame_bytes])
        del self._partner_buf[:frame_bytes]
        del self._agent_buf[:frame_bytes]

        if len(p_chunk) < frame_bytes:
            p_chunk += b"\x00" * (frame_bytes - len(p_chunk))
        if len(a_chunk) < frame_bytes:
            a_chunk += b"\x00" * (frame_bytes - len(a_chunk))

        mixed = self._mix_pcm16(p_chunk, a_chunk)
        self._ensure_open()
        self._wav.writeframes(mixed)
        self._frames_written += 1

    async def write_partner_mulaw(self, mulaw_bytes: bytes) -> None:
        """Queue partner audio for mixing. Decoded to PCM16 immediately."""
        if not mulaw_bytes:
            return
        async with self._lock:
            self._partner_buf.extend(_mulaw_to_pcm16(mulaw_bytes))

    async def write_agent_mulaw(self, mulaw_bytes: bytes) -> None:
        """
        Queue agent (TTS) audio for mixing. Decoded to PCM16 immediately.
        Deliberately does NOT flush here (see class docstring) — a burst
        of agent audio just sits here until the periodic loop drains it
        at the correct real-time pace.
        """
        if not mulaw_bytes:
            return
        async with self._lock:
            self._agent_buf.extend(_mulaw_to_pcm16(mulaw_bytes))

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
        """Stop the periodic loop, flush any final partial frame, finalise the WAV."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        async with self._lock:
            # Drain any remaining COMPLETE frames first — the periodic
            # loop may have been cancelled with more than one frame's
            # worth still queued (e.g. a burst that arrived right as the
            # call ended), and those still represent real audio that
            # should be kept, not discarded.
            frame_bytes = self._FRAME_SAMPLES * 2
            while len(self._partner_buf) >= frame_bytes or len(self._agent_buf) >= frame_bytes:
                self._write_one_frame()

            # Final partial frame — pad out whatever's left (<20ms from
            # either side) so we don't lose it entirely.
            if self._partner_buf or self._agent_buf:
                self._write_one_frame()

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
    We respond with TwiML that opens the bidirectional media stream
    IMMEDIATELY — no Twilio <Say> verb before it.

    CRITICAL: the opening line is no longer spoken via Twilio's native
    <Say> verb. It is now sent through our OWN TTS pipeline once the
    WebSocket connects (see audio_stream() -> recv_from_twilio()'s
    "start" event handler). This fixes a real bug: our CallRecorder only
    captures audio flowing through the bidirectional media stream —
    anything spoken via <Say> happens BEFORE that stream exists and was
    structurally invisible to the recorder, making it look like
    "recording only starts after the client speaks" when actually the
    agent's own opening line was simply never being captured at all.
    Speaking it through our own pipeline also means barge-in detection
    is active from the very first word, instead of only starting once
    the (now-removed) <Say> + pause finished.
    """
    form = await request.form()
    call_sid = form.get("CallSid", "unknown")
    meta = _calls.get(call_sid, {})

    await _db_upsert_call(call_sid, status="connected", from_number=form.get("From", ""))
    logger.info("Call connected SID=%s", call_sid)

    # Opening line — exact wording per explicit instruction. Stored on
    # meta and spoken later via our own pipeline (see above), not here.
    #
    # "Hi, I am Sania from Aarna - a travel management platform. Are you
    #  {client_name} from {company_name}?"
    #
    # Degrades gracefully when contact_name and/or partner_name are
    # missing, rather than saying "None" or leaving an awkward blank.
    contact_name = meta.get("contact_name") or ""
    partner_name = meta.get("partner_name") or ""

    if contact_name and partner_name:
        opening = (
            f"Hi, I am Sania, an AI assistant from Aarna - a travel management platform. "
            f"Are you {contact_name} from {partner_name}?"
        )
    elif partner_name:
        opening = (
            f"Hi, I am Sania, an AI assistant from Aarna - a travel management platform. "
            f"Am I speaking with the right person at {partner_name}?"
        )
    else:
        opening = (
            "Hi, I am Sania, an AI assistant from Aarna - a travel management platform. "
            "Am I speaking with the right person to discuss a potential "
            "partnership?"
        )

    opening = meta.get("script") or opening
    meta["opening_line"] = opening

    response = VoiceResponse()

    # Open bidirectional media stream immediately — no <Say>, no pause.
    # The opening line is spoken over this same stream once it connects.
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
    if recorder is not None:
        recorder.start()

    # _speaking: True while we're generating + sending TTS audio.
    _speaking = False
    # Wall-clock time the FIRST actual audio chunk was sent to Twilio —
    # i.e. when the caller could first possibly hear anything. Deliberately
    # NOT set at the top of _send_tts_reply (before TTS synthesis runs):
    # synthesis alone can take 1-2+ seconds during which NO audio has
    # reached the caller at all, so any "barge-in" detected during that
    # window is necessarily a false positive (background noise, line
    # artifact) — there is nothing yet for the caller to be interrupting.
    # Stays 0.0 (falsy) for the whole synthesis phase, which means the
    # barge-in checks below simply don't fire at all until real audio has
    # started, rather than needing a grace window measured from the wrong
    # starting point. This is the fix for a confirmed real case: a
    # "Barge-in signal (VAD)" fired ~600ms after TTS synthesis STARTED
    # (well before the ~1954ms synthesis even finished, so zero audio had
    # been sent yet) and silently discarded the entire reply before the
    # send-loop's first chunk.
    _speaking_started_at: float = 0.0
    # Minimum time the agent must have been speaking before a barge-in
    # signal is trusted. Without this, the caller's own phone mic picking
    # up an echo of the agent's OWN voice (very common on real phone
    # lines without hardware echo cancellation) gets misread as the
    # caller interrupting almost instantly — cutting the agent off before
    # it ever gets a full sentence out. This was very likely the actual
    # cause of "the agent wasn't speaking anything" — audio WAS generated
    # and sent, but a self-triggered false barge-in wiped it from Twilio's
    # buffer within a fraction of a second.
    _BARGE_IN_GRACE_S = 0.7
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
    # This is t=0 for the WHOLE reply (used for the "Groq first sentence
    # ready" metric, which is meant to be utterance-level).
    _final_transcript_ts: float = 0.0
    # _sentence_ref_ts: reset before EACH sentence in a multi-sentence
    # reply, right before that sentence is sent to _send_tts_reply. Used
    # for the TTS-synth/audio-ready timing logs so they report real
    # PER-SENTENCE delay. Without this, those logs used
    # _final_transcript_ts for every sentence, so a later sentence in a
    # long reply reported a misleadingly huge cumulative number (the
    # elapsed time for the WHOLE reply so far, not that sentence's own
    # delay) — confirmed real case: "END-TO-END: 20125ms" on a 4th
    # sentence that individually only took ~950ms to synthesize.
    _sentence_ref_ts: float = 0.0

    async def _send_tts_reply(text: str, stream_sid: str) -> bool:
        """
        Synthesise text and stream mulaw audio back to Twilio.
        Returns True if playback completed fully, False if it was cut short
        by a barge-in (caller can check this to decide how to handle the
        next turn).
        """
        nonlocal _speaking, _interrupt_flag, _speaking_started_at
        _interrupt_flag = False
        _speaking = True
        _speaking_started_at = 0.0   # stays 0 until first chunk actually sent — see comment above
        completed = True

        # ── Timing: TTS synthesis stage ──────────────────────────────────
        t_tts_start = time.monotonic()
        try:
            logger.info("[AGENT] %s", text)
            mulaw = await _tts_to_mulaw(text)
            t_tts_done = time.monotonic()
            tts_synth_ms = (t_tts_done - t_tts_start) * 1000

            # If we know when this sentence became ready to speak, log the
            # FULL chain latency for THIS sentence: ready -> TTS audio ready.
            # This isolates whether edge-tts itself is the slow stage, per
            # sentence — not cumulative across a whole multi-sentence reply.
            if _sentence_ref_ts:
                total_to_audio_ready_ms = (t_tts_done - _sentence_ref_ts) * 1000
                logger.info(
                    "TIMING SID=%s — TTS synth: %.0fms | sentence→audio_ready: %.0fms",
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
                        _speaking_started_at = time.monotonic()   # real audio starts NOW
                        if _sentence_ref_ts:
                            total_e2e_ms = (time.monotonic() - _sentence_ref_ts) * 1000
                            logger.info(
                                "TIMING SID=%s — END-TO-END this sentence (ready → first audio sent): %.0fms",
                                call_sid, total_e2e_ms,
                            )

                    # Record agent audio as it's actually sent — keeps the
                    # recording in true chronological sync with what the
                    # partner heard, including any chunk that was cut short
                    # by a barge-in (we only record what was actually played).
                    if recorder is not None:
                        await recorder.write_agent_mulaw(chunk)

                    # REVERTED to a plain yield (not a real per-chunk delay).
                    # A previous fix paced this at exactly chunk_duration_s
                    # per chunk to keep `_speaking` accurate — but that
                    # removed all buffer slack: Twilio, Deepgram, the
                    # recorder, and DB writes all share this event loop, and
                    # ANY scheduling jitter meant a chunk could arrive after
                    # its predecessor finished playing, causing audible
                    # gaps/choppiness. Confirmed real complaint: "the voice
                    # is extremely breaking." Dispatch is fast again (fills
                    # Twilio's buffer immediately, immune to our own jitter);
                    # `_speaking` accuracy is now maintained separately below
                    # by waiting out the real remaining duration AFTER all
                    # chunks are sent, instead of pacing the sends themselves.
                    await asyncio.sleep(0)

                # ── Wait out the REAL remaining playback duration ─────────
                # All chunks are now sent (Twilio has the full buffer and
                # will play it smoothly at its own pace). But `_speaking`
                # must stay True until that audio actually finishes playing
                # — otherwise barge-in/silence-nudge logic goes back to
                # reading a flag that doesn't reflect reality (the original
                # bug). Poll in short steps so a barge-in signal arriving
                # near the end of playback still gets caught promptly and
                # can still send a `clear` event.
                if completed and mulaw:
                    total_audio_duration_s = len(mulaw) / 8000.0
                    elapsed_dispatch_s = time.monotonic() - t_tts_done
                    remaining_s = total_audio_duration_s - elapsed_dispatch_s
                    poll_s = 0.15
                    waited_s = 0.0
                    while remaining_s - waited_s > 0:
                        if _interrupt_flag:
                            logger.info(
                                "Barge-in detected during playback tail SID=%s — cutting off",
                                call_sid,
                            )
                            try:
                                await ws.send_text(_clear_message(stream_sid))
                            except Exception:
                                pass
                            completed = False
                            break
                        step_s = min(poll_s, remaining_s - waited_s)
                        await asyncio.sleep(step_s)
                        waited_s += step_s
        except Exception as exc:
            logger.error("TTS send error SID=%s: %s", call_sid, exc)
        finally:
            _speaking = False
        return completed

    async def _handle_utterance(text: str, speaker: str) -> None:
        """
        Process one STT utterance through intent detection → LLM → TTS pipeline.
        """
        nonlocal _speaking, _interrupt_flag, _last_utterance_ts, _final_transcript_ts, _sentence_ref_ts
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
            _this_sentence_ts = time.monotonic()
            _sentence_ref_ts = _this_sentence_ts  # reset — t=0 for THIS sentence's own timing logs
            # ── Timing: Groq first-sentence latency ───────────────────────
            # Logged only for the first sentence of each reply — this isolates
            # how long Groq took to generate the first usable chunk of text,
            # independent of how many sentences the full reply has.
            if _sentence_index == 1:
                groq_first_sentence_ms = (_this_sentence_ts - _final_transcript_ts) * 1000
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

        FIX: previously this slept a flat SILENCE_NUDGE_S from when the
        task was CREATED (near call start), then — once real-time playback
        duration made `_speaking` accurate — separately waited for speaking
        to stop before firing IMMEDIATELY once it did. That meant the
        opening line itself (several real seconds to speak) ate almost the
        entire silence budget, so the nudge fired moments after the caller
        could first possibly respond, not after SILENCE_NUDGE_S of them
        actually saying nothing. Confirmed real complaint: "it directly
        asks 'are you there' without even listening." Now the clock only
        starts counting once the agent has genuinely stopped speaking, and
        resets to zero any time speaking resumes (e.g. the opening line
        itself), so the caller always gets a full SILENCE_NUDGE_S of real
        quiet time before the nudge fires.
        """
        nonlocal _last_utterance_ts
        poll_s = 0.25
        quiet_since: float | None = None
        max_total_wait_s = 45.0
        total_waited_s = 0.0

        while total_waited_s < max_total_wait_s:
            await asyncio.sleep(poll_s)
            total_waited_s += poll_s

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
                quiet_since = None   # agent is talking — reset the quiet-time clock
                continue

            if quiet_since is None:
                quiet_since = time.monotonic()
                continue

            if time.monotonic() - quiet_since >= SILENCE_NUDGE_S:
                break
        else:
            return   # hit the safety cap without ever qualifying — give up quietly

        logger.info(
            "Silence nudge firing SID=%s — %ds of genuine silence after the agent stopped speaking",
            call_sid, SILENCE_NUDGE_S,
        )
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

                        # Speak the opening line NOW, through our own TTS
                        # pipeline, over this same stream — this is what
                        # makes it show up in the recording and makes
                        # barge-in detection active from the very first
                        # word (previously impossible, since the opening
                        # played via Twilio's <Say> before this stream, or
                        # even Deepgram, existed at all). Fired as a
                        # background task so it doesn't block this loop
                        # from continuing to forward the partner's own
                        # audio to Deepgram concurrently.
                        opening_line = meta.get("opening_line", "")
                        if opening_line:
                            _opening_ts = _utc_naive()
                            meta["transcript"].append({
                                "speaker": "Agent", "text": opening_line,
                                "ts": _opening_ts.isoformat(),
                            })
                            asyncio.create_task(_db_insert_line(call_sid, "Agent", opening_line, _opening_ts))
                            asyncio.create_task(
                                _send_tts_reply(opening_line, meta["stream_sid"])
                            )

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
                    msg_type = result.get("type")

                    # ── SpeechStarted (requires vad_events=true) — LOGGED
                    # ONLY, does NOT trigger barge-in. ──
                    #
                    # Previously this fired an immediate barge-in on pure
                    # acoustic activity, with no transcription required, on
                    # the theory that it's faster than waiting for Results.
                    # Confirmed real-world failure: a live call log showed
                    # THREE consecutive barge-ins, ALL of the VAD-only type,
                    # with NO corresponding transcribed partner speech in two
                    # of the three cases (one followed by 14 seconds of dead
                    # silence before a confused "Hello?"). This is the
                    # signature of acoustic echo — the caller's phone mic
                    # picking up the agent's OWN voice and sending it back
                    # up the line, which Deepgram's VAD correctly flags as
                    # "someone started talking" even though there are no
                    # real words, because there aren't any. Twilio Media
                    # Streams does not do acoustic echo cancellation between
                    # what it plays out and what it captures back in — that
                    # is left entirely to the callee's device, which mobile
                    # phones on speaker (or certain call-forwarding setups)
                    # often don't handle well.
                    #
                    # Fix: only the Results branch below (which requires
                    # Deepgram to have actually transcribed real words) may
                    # set _interrupt_flag now. An echo of the agent's own
                    # voice essentially never resolves into coherent
                    # transcribed speech; a genuine interruption does,
                    # usually within a few hundred ms via interim_results.
                    if msg_type == "SpeechStarted":
                        logger.debug(
                            "SpeechStarted (VAD) SID=%s — logged only, not treated as barge-in "
                            "(see comment: this fires on echo without real words too often).",
                            call_sid,
                        )
                        continue

                    if msg_type != "Results":
                        continue

                    alts = result.get("channel", {}).get("alternatives", [])
                    if not alts:
                        continue

                    text = alts[0].get("transcript", "").strip()
                    # Require at least 2 characters — filters out stray
                    # single-character noise blips Deepgram occasionally
                    # transcribes from line static, without needing to
                    # wait for a full word.
                    if not text or len(text) < 2:
                        continue

                    is_final = result.get("is_final", False)

                    # ── Barge-in detection — the ONLY trigger now (see the
                    # SpeechStarted comment above for why the VAD-only path
                    # was disabled). This still fires on interim (non-final)
                    # results, so it's still fast — Deepgram typically
                    # produces an interim result within a few hundred ms of
                    # real speech starting — but it requires ACTUAL
                    # transcribed content, which an echo of the agent's own
                    # voice essentially never produces.
                    if (_speaking and not _interrupt_flag and _speaking_started_at
                            and (time.monotonic() - _speaking_started_at) >= _BARGE_IN_GRACE_S):
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