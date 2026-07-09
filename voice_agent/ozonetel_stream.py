"""
voice_agent/ozonetel_stream.py
--------------------------------
WebSocket endpoint for Ozonetel's call-streaming integration.

STATUS: diagnostic stub. Ozonetel's public docs don't specify their
bidirectional voicebot streaming message format the way Twilio/Exotel's
do, so this endpoint's first job is simply to ACCEPT the connection and
LOG every raw message Ozonetel sends — once they place a real test call
through it, the logs will show their actual event names, audio encoding,
and message shape. Use that to fill in the TODOs below and turn this into
a real handler (mirroring the Twilio flow in engine.py's twilio_stream
handler once the protocol is confirmed).

Wire this into your FastAPI app the same way engine.py's router is
registered in main.py, e.g.:
    from voice_agent.ozonetel_stream import router as ozonetel_router
    app.include_router(ozonetel_router)
"""

import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ozonetel/stream")
async def ozonetel_stream(ws: WebSocket):
    await ws.accept()
    logger.info("Ozonetel stream: connection accepted from %s", ws.client)

    try:
        while True:
            raw = await ws.receive()

            # FastAPI's receive() can hand back either text or bytes —
            # log both cases distinctly so we can tell from the logs
            # whether Ozonetel sends JSON-framed events (like Twilio/
            # Exotel) or raw binary audio frames directly.
            if "text" in raw and raw["text"] is not None:
                logger.info("Ozonetel stream: TEXT message: %s", raw["text"])
                try:
                    parsed = json.loads(raw["text"])
                    event = parsed.get("event") or parsed.get("type")
                    logger.info("Ozonetel stream: parsed event=%r keys=%s", event, list(parsed.keys()))
                    # TODO once protocol is confirmed: dispatch on `event`
                    # the same way twilio_stream in engine.py dispatches
                    # on "start" / "media" / "stop".
                except json.JSONDecodeError:
                    logger.info("Ozonetel stream: text message was not JSON.")

            elif "bytes" in raw and raw["bytes"] is not None:
                logger.info("Ozonetel stream: BINARY message, %d bytes", len(raw["bytes"]))
                # TODO once protocol is confirmed: this is likely raw
                # audio (codec/sample rate TBD from Ozonetel) — feed it
                # into the same STT pipeline engine.py's Twilio handler
                # uses, once we know the encoding.

            elif raw.get("type") == "websocket.disconnect":
                logger.info("Ozonetel stream: disconnect received.")
                break

    except WebSocketDisconnect:
        logger.info("Ozonetel stream: WebSocket disconnected.")
    except Exception as exc:
        logger.error("Ozonetel stream: unexpected error — %s", exc)