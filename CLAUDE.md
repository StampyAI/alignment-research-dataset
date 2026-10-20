# Notes for contributors (and Claude)

## Retry policy

The daily/weekly fetch+index jobs hit external services (Coda, GreaterWrong
GraphQL, the MySQL article DB, Pinecone) that fail transiently. The standing
policy for retries:

- **Simple stateless transient retry**: use `align_data/common/retry.py:with_retry`.
  One canonical decorator, exponential backoff, re-raises the original exception
  on exhaustion. Filter to transient exceptions only (Timeout, ConnectionError);
  let HTTP-status and schema errors fail fast (a 404 won't self-resolve).
- **Pinecone 429 rate-limit ladder**: `with_pinecone_retry` in
  `pinecone_db_handler.py`. Per-exception attempt counts (429 backs off for
  hours, ProtocolError 3x, other errors no retry). tenacity can't express this
  ladder, which is why it stays bespoke.
- **Stateful retry** (e.g. a dropped DB connection needs `session.rollback()`
  between attempts): bespoke loops, e.g. `_read_with_retry` in
  `update_pinecone.py`.

Guiding constraint: a failed job must be correct or loud, never silently leave
MySQL and Pinecone disagreeing. Writes are idempotent (upsert + `pinecone_status`),
so a crash leaves rows `pending` for the next run rather than dropping vectors.

## Adding a utility pattern

Before adding a new retry or util variant, grep for existing ones and fold into
the canonical version. Keep at most one general implementation per task. Run a
parsimony pass per change: does this fold into something that already exists?
