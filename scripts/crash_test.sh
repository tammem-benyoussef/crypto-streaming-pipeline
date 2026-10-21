#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

mode="${CRASH_MODE:-idle}"
log_file="${CRASH_LOG:-${TMPDIR:-/tmp}/bars-crash-test-${mode}.$$}.log"
job_pid=

cleanup() {
    if [[ -n "$job_pid" ]] && kill -0 "$job_pid" 2>/dev/null; then
        kill "$job_pid" 2>/dev/null || true
        wait "$job_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

case "$mode" in
    idle)
        ;;
    mid-batch)
        export BARS_TEST_SLEEP_BEFORE_WRITE_SECONDS="${BARS_TEST_SLEEP_BEFORE_WRITE_SECONDS:-60}"
        ;;
    mid-write)
        export BARS_WRITE_SLEEP_SECONDS="${BARS_WRITE_SLEEP_SECONDS:-60}"
        ;;
    *)
        echo "unsupported CRASH_MODE: $mode (expected idle, mid-batch, or mid-write)" >&2
        exit 2
        ;;
esac

: "${CRASH_WAIT_SECONDS:=30}"
: "${RESTART_WAIT_SECONDS:=210}"
: "${VERIFY_SINCE_MINUTES:=60}"
: "${MIN_COMPARED_BARS:=30}"

printf 'START mode=%s at=%s\n' "$mode" "$(date --iso-8601=seconds)"
./scripts/run_bars_job.sh >"$log_file" 2>&1 &
job_pid=$!

sleep "$CRASH_WAIT_SECONDS"
if ! kill -0 "$job_pid" 2>/dev/null; then
    echo "INCONCLUSIVE: streaming job exited before the scheduled kill" >&2
    exit 3
fi
printf 'KILL pid=%s at=%s\n' "$job_pid" "$(date --iso-8601=seconds)"
kill -9 "$job_pid"
wait "$job_pid" 2>/dev/null || true
job_pid=

./scripts/run_bars_job.sh >>"$log_file" 2>&1 &
job_pid=$!
sleep "$RESTART_WAIT_SECONDS"
if ! kill -0 "$job_pid" 2>/dev/null; then
    echo "INCONCLUSIVE: restarted streaming job exited before verification" >&2
    exit 3
fi

set +e
verify_output=$(python scripts/verify_bars.py "$VERIFY_SINCE_MINUTES" 2>&1)
result=$?
set -e
printf '%s\n' "$verify_output"
if [[ "$result" -ne 0 ]]; then
    echo "RESULT: MISMATCH"
    exit "$result"
fi
compared=$(awk '/^compared [0-9]+ bars / {print $2; exit}' <<<"$verify_output")
if [[ -z "$compared" || "$compared" -lt "$MIN_COMPARED_BARS" ]]; then
    echo "INCONCLUSIVE: expected at least $MIN_COMPARED_BARS compared bars, got ${compared:-none}" >&2
    exit 3
fi
echo "RESULT: OK"
