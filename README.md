# A2A Invoice Action Agent (starter)

FastAPI implementation of the invoice-action A2A HTTP+JSON surface.

## Important deployment note
Render's free web services have ephemeral filesystems and can sleep. This starter points SQLite at `/var/data`, which requires a persistent disk on a paid Render plan. For free deployment, use a persistent external PostgreSQL/SQLite-compatible database or another durable store before running graded checks. Do not rely on local SQLite on an ephemeral host.

## Local run
```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:AGENT_BEARER_TOKEN="replace-with-long-random-token"
$env:AI_API_KEY="your-provider-key"
$env:A2A_BASE_URL="https://your-host/a2a"
uvicorn main:app --reload --port 8000
```

The public card is `GET /.well-known/agent-card.json`. A2A routes are under `/a2a/`.

All non-card endpoints require:
- `Authorization: Bearer <AGENT_BEARER_TOKEN>`
- `A2A-Version: 1.0`
- POST `Content-Type: application/a2a+json`

## Before using a graded Check or Save
This is a starter, not a verified full-credit submission. In particular:
- Configure truly durable storage and verify transaction semantics for the hosting environment.
- Test message-id deduplication for result continuations and terminal replay.
- Review the generated Agent Card URL against the exact URL submitted to the grader.
- Harden package schema handling, duplicate-package cache behavior, and action-id stability.
- Test simultaneous cancel and result continuations.
- Confirm the grader's exact task status/envelope expectations.
- Run local contract tests with synthetic invoices before spending a check.

Never use real invoices, finance records, or production secrets in this assignment.
