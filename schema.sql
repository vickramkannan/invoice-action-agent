CREATE TABLE IF NOT EXISTS tasks (
  principal_hash TEXT NOT NULL,
  task_id TEXT PRIMARY KEY,
  context_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  state TEXT NOT NULL,
  task_json JSONB NOT NULL,
  proposal_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tasks_owner_created ON tasks(principal_hash, created_at DESC);
CREATE TABLE IF NOT EXISTS message_dedup (
  principal_hash TEXT NOT NULL,
  message_id TEXT NOT NULL,
  message_hash TEXT NOT NULL,
  task_id TEXT NOT NULL,
  response_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(principal_hash, message_id)
);
CREATE TABLE IF NOT EXISTS decision_cache (
  canonical_hash TEXT PRIMARY KEY,
  decision_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
