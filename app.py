"""
Convoso Claude Summary Service

Receives disposition webhooks from Convoso, generates a professional
CRM summary using Claude, and writes the summary back to the lead's
notes field in Convoso.

Flow:
  Convoso disposition fires webhook -> /webhook endpoint
  -> Parse the call data (handles both JSON and form-encoded)
  -> If lead_id is missing, look it up by phone number
  -> Call Anthropic API to generate summary
  -> POST summary to Convoso /leads/update
  -> Done. Notes field on the lead now has Claude's summary.
"""

import json
import logging
import os
import urllib.parse

import anthropic
import requests
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("convoso-claude")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
CONVOSO_AUTH_TOKEN = os.environ.get("CONVOSO_AUTH_TOKEN")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
CONVOSO_API_BASE = "https://api.convoso.com/v1"

if not ANTHROPIC_API_KEY:
    log.warning("ANTHROPIC_API_KEY is not set!")
if not CONVOSO_AUTH_TOKEN:
    log.warning("CONVOSO_AUTH_TOKEN is not set!")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ---------------------------------------------------------------------------
# Webhook payload parsing
# ---------------------------------------------------------------------------

def parse_convoso_payload(req):
    """Extract a dict of call data from the incoming request, no matter
    how Convoso wraps it."""
    content_type = (req.content_type or "").lower()
    raw_body = req.get_data(as_text=True)

    log.info("Incoming Content-Type: %s", content_type)
    log.info("Raw body (first 500 chars): %s", raw_body[:500])

    if "application/json" in content_type:
        try:
            return req.get_json(force=True, silent=False)
        except Exception as e:
            log.warning("Failed to parse as JSON despite Content-Type: %s", e)

    if "application/x-www-form-urlencoded" in content_type or "=" in raw_body:
        decoded = urllib.parse.unquote_plus(raw_body)
        candidate = decoded.rstrip("=").strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

        form_data = req.form.to_dict()
        if form_data:
            if len(form_data) == 1:
                only_key = next(iter(form_data.keys()))
                if only_key.startswith("{"):
                    try:
                        return json.loads(only_key)
                    except json.JSONDecodeError:
                        pass
            return form_data

    stripped = raw_body.strip().rstrip("=")
    if stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse payload. Body: {raw_body[:300]}")


# ---------------------------------------------------------------------------
# Convoso lead lookup by phone number
# ---------------------------------------------------------------------------

def normalize_phone(phone):
    """Strip everything that isn't a digit."""
    if not phone:
        return ""
    return "".join(c for c in str(phone) if c.isdigit())


def lookup_lead_id_by_phone(phone_number):
    """Search Convoso for a lead by phone number and return the lead_id."""
    phone_digits = normalize_phone(phone_number)
    if not phone_digits:
        raise ValueError("Cannot lookup - phone number is empty")

    # Strip leading 1 from US 11-digit numbers
    if len(phone_digits) == 11 and phone_digits.startswith("1"):
        phone_digits = phone_digits[1:]

    url = f"{CONVOSO_API_BASE}/leads/search"
    payload = {
        "auth_token": CONVOSO_AUTH_TOKEN,
        "phone_number": phone_digits,
        "limit": 5,
        "offset": 0,
    }

    log.info("Looking up lead by phone: %s", phone_digits)
    resp = requests.post(url, data=payload, timeout=30)

    try:
        body = resp.json()
    except ValueError:
        raise RuntimeError(f"Convoso lookup returned non-JSON: {resp.text[:200]}")

    if not body.get("success"):
        raise RuntimeError(f"Convoso lookup failed: {body}")

    data = body.get("data", {})
    entries = data.get("entries", []) if isinstance(data, dict) else []

    if not entries:
        raise RuntimeError(f"No lead found for phone {phone_digits}")

    def sort_key(lead):
        return (
            lead.get("modified_at") or "",
            int(lead.get("id", 0)) if str(lead.get("id", "0")).isdigit() else 0,
        )
    entries.sort(key=sort_key, reverse=True)

    lead_id = entries[0].get("id")
    log.info("Found lead_id=%s for phone %s (out of %d matches)",
             lead_id, phone_digits, len(entries))
    return lead_id


def resolve_lead_id(call_data):
    """Get the lead_id either from the payload or by phone lookup."""
    lead_id = call_data.get("lead_id")
    if lead_id:
        log.info("lead_id found directly in payload: %s", lead_id)
        return lead_id

    phone = (call_data.get("phone_number_call")
             or call_data.get("phone_number")
             or call_data.get("phone"))

    if not phone:
        raise ValueError("Payload has no lead_id and no phone number to look up")

    return lookup_lead_id_by_phone(phone)


# ---------------------------------------------------------------------------
# Claude summary generation
# ---------------------------------------------------------------------------

SUMMARY_SYSTEM_PROMPT = """You are an expert call center analyst writing CRM notes.

Generate a concise, professional 2-3 sentence summary of a call based on the
data provided. Focus on:
  - The outcome (what disposition, what happened)
  - The agent's action
  - Any next steps or follow-up needed

Rules:
  - No preamble. Start directly with the summary.
  - No "**CRM Interaction Note:**" headers or markdown.
  - No disclaimers about missing data - work with what you have.
  - If data is sparse, write a brief factual note. Don't pad with filler.
  - Professional tone. No emojis."""


def generate_summary(call_data):
    """Send the call data to Claude and get a CRM summary back."""
    if not claude:
        raise RuntimeError("Anthropic client is not configured")

    relevant_fields = {
        "Lead first name": call_data.get("first_name"),
        "Lead last name": call_data.get("last_name"),
        "Phone": call_data.get("phone_number") or call_data.get("phone_number_call"),
        "Disposition": call_data.get("disposition") or call_data.get("status"),
        "Call duration (seconds)": call_data.get("length_in_sec"),
        "Agent": call_data.get("agent_full_name"),
        "Campaign": call_data.get("campaign_name"),
        "Call date": call_data.get("call_date"),
        "Call type": call_data.get("call_type"),
        "Termination reason": call_data.get("term_reason"),
        "Agent comment": call_data.get("agent_comment"),
        "Agent notes": call_data.get("agent_notes"),
    }

    lines = [f"{label}: {value}" for label, value in relevant_fields.items()
             if value not in (None, "", 0)]

    if not lines:
        return "Call completed. No additional details captured."

    user_message = "Call data:\n" + "\n".join(lines)
    log.info("Sending %d fields to Claude", len(lines))

    response = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=300,
        system=SUMMARY_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    summary = response.content[0].text.strip()
    log.info("Claude generated %d char summary", len(summary))
    return summary


# ---------------------------------------------------------------------------
# Convoso write-back
# ---------------------------------------------------------------------------

def update_convoso_lead_notes(lead_id, notes):
    """POST the summary to Convoso's leads/update endpoint."""
    if not CONVOSO_AUTH_TOKEN:
        raise RuntimeError("CONVOSO_AUTH_TOKEN is not configured")

    url = f"{CONVOSO_API_BASE}/leads/update"
    payload = {
        "auth_token": CONVOSO_AUTH_TOKEN,
        "lead_id": str(lead_id),
        "notes": notes,
    }

    log.info("Posting to Convoso for lead_id=%s", lead_id)
    resp = requests.post(url, data=payload, timeout=30)

    try:
        body = resp.json()
    except ValueError:
        body = {"raw": resp.text}

    log.info("Convoso responded: status=%s body=%s", resp.status_code, body)

    if resp.status_code != 200 or not body.get("success", False):
        raise RuntimeError(f"Convoso update failed: {body}")

    return body


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    """Render's health check hits this."""
    return jsonify({
        "service": "convoso-claude-summary",
        "status": "running",
        "endpoints": {
            "POST /webhook": "Receives Convoso disposition webhooks",
            "GET /": "This health check",
        },
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    """Main entrypoint for Convoso disposition webhooks."""
    try:
        call_data = parse_convoso_payload(request)
    except ValueError as e:
        log.error("Payload parse failed: %s", e)
        return jsonify({"ok": False, "error": "bad_payload", "detail": str(e)}), 400

    log.info("Parsed payload keys: %s", list(call_data.keys()))

    try:
        lead_id = resolve_lead_id(call_data)
    except Exception as e:
        log.exception("Could not resolve lead_id")
        return jsonify({"ok": False, "error": "lead_id_unresolvable",
                        "detail": str(e)}), 400

    try:
        summary = generate_summary(call_data)
    except Exception as e:
        log.exception("Claude call failed")
        return jsonify({"ok": False, "error": "claude_failed",
                        "detail": str(e)}), 500

    try:
        result = update_convoso_lead_notes(lead_id, summary)
    except Exception as e:
        log.exception("Convoso update failed")
        return jsonify({
            "ok": False,
            "error": "convoso_update_failed",
            "detail": str(e),
            "summary_that_would_have_been_saved": summary,
        }), 500

    return jsonify({
        "ok": True,
        "lead_id": lead_id,
        "summary": summary,
        "convoso_response": result,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
