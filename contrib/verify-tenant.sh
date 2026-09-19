#!/usr/bin/env bash
#
# chronicle-verify-tenant — the synthetic tenant's could-not-succeed-if-
# broken read (chronicle-hq/02-DESIGN/09-hosted-environment.md § the
# stand-up ceremony): a birth appended, its state folded and read back at
# or past the ack's sequence, and the declared search index answering a
# query for a token that exists nowhere else. It speaks the CLI exactly as
# a tenant does, through a saved context, so it proves the public path —
# not a private one.
#
# Prerequisites, once, as the user this runs as:
#   chronicle context save "$CHRONICLE_CONTEXT" --creds <synthetic admin creds> --url <public url>
#   chronicle log create "$CHRONICLE_LOG" --context "$CHRONICLE_CONTEXT"
#   chronicle index declare "$CHRONICLE_INDEX" --kind search --context "$CHRONICLE_CONTEXT" --log "$CHRONICLE_LOG"
#
# Exit 0 with one summary line when all three hold within the deadline;
# non-zero, naming the step that did not, otherwise.
set -euo pipefail

CONTEXT="${CHRONICLE_CONTEXT:-synthetic}"
LOG="${CHRONICLE_LOG:-probe}"
INDEX="${CHRONICLE_INDEX:-text}"
DEADLINE="${VERIFY_DEADLINE_SECONDS:-60}"
CHRONICLE="${CHRONICLE_BIN:-chronicle}"

# Alphanumeric only: one search token, matching this run and nothing else.
stamp="p$(date -u +%Y%m%dt%H%M%Sz)$RANDOM"
thing="probe.$stamp"
start=$(date +%s)

elapsed() { echo $(( $(date +%s) - start )); }
fail() { echo "verify FAILED after $(elapsed)s at $1: $2" >&2; exit 1; }
within_deadline() { [ "$(elapsed)" -lt "$DEADLINE" ]; }

# 1. Append: the birth's ack carries the sequence the fold must reach.
born="$("$CHRONICLE" create "$thing" --payload "{\"note\":\"$stamp\"}" --context "$CONTEXT" --log "$LOG" 2>&1)" \
  || fail "append" "$born"
seq="$(sed -n 's/^born: .* at seq \([0-9][0-9]*\).*/\1/p' <<<"$born")"
[ -n "$seq" ] || fail "append" "no sequence in: $born"

# 2. Fold → state: the state read answers at or past that sequence.
while :; do
  if state="$("$CHRONICLE" get "$thing" --context "$CONTEXT" --log "$LOG" 2>/dev/null)"; then
    got="$(sed -n '1s/^seq \([0-9][0-9]*\)$/\1/p' <<<"$state")"
    if [ -n "$got" ] && [ "$got" -ge "$seq" ]; then break; fi
  fi
  within_deadline || fail "state" "$thing never folded to seq $seq"
  sleep 1
done

# 3. Index → query: the declared search index returns this thing for its token.
while :; do
  if hits="$("$CHRONICLE" query "$INDEX" "$stamp" --context "$CONTEXT" --log "$LOG" 2>/dev/null)" \
     && grep -q "^$thing"$'\t' <<<"$hits"; then
    break
  fi
  within_deadline || fail "query" "index $INDEX never returned $thing"
  sleep 1
done

echo "verify ok: $thing at seq $seq folded and indexed in $(elapsed)s (context $CONTEXT, log $LOG, index $INDEX)"
