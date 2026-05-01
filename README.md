# Convoso Claude Summary Service

Receives Convoso disposition webhooks, generates a CRM summary using Claude, and writes it back to the lead's Notes field.

## What it does

1. Convoso fires a webhook when a disposition is saved
2. This service receives the call data
3. Claude generates a 2-3 sentence professional summary
4. The summary is posted back to Convoso's `/leads/update` endpoint
5. The Notes field on the lead now shows the summary

## Environment variables

Set these in your hosting environment (Render dashboard):

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Your Anthropic API key (starts with `sk-ant-`) |
| `CONVOSO_AUTH_TOKEN` | Your Convoso API auth token |
| `CLAUDE_MODEL` | Optional. Defaults to `claude-sonnet-4-5-20250929` |

## Endpoints

- `GET /` — health check / status
- `POST /webhook` — main webhook endpoint for Convoso

## Deploying to Render

1. Push this repo to GitHub
2. In Render: New → Web Service → connect the GitHub repo
3. Render auto-detects Python and reads `Procfile`
4. Add environment variables (above) in the Render dashboard
5. Deploy
6. Copy the Render URL (something like `https://your-app.onrender.com`)
7. In Convoso → Apps → Convoso Connect → Make Claude Webhook, replace the API URL with `https://your-app.onrender.com/webhook`
8. Save

## Required Convoso webhook fields

The Convoso adaptor must include at minimum:
- `lead_id` (required for the writeback)

The summary improves with these optional fields:
- `first_name`, `last_name`
- `disposition` or `status`
- `length_in_sec`
- `agent_full_name`
- `campaign_name`
- `call_date`
- `term_reason`
- `agent_comment`
- `agent_notes`

## Local development

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
export CONVOSO_AUTH_TOKEN=...
python app.py
```

Service runs on http://localhost:8000
