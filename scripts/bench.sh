#!/usr/bin/env bash
set -euo pipefail

port=${1:?usage: bench.sh PORT [PIPELINE] [RUNS]}
pipeline=${2:-1}
runs=${3:-3}
requests=${REQUESTS:-200000}
clients=${CLIENTS:-50}
tests=${TESTS:-ping,set,get,incr,lpush,rpush,lpop,rpop,mset}

if command -v redis-benchmark >/dev/null; then
	bench=(redis-benchmark)
else
	bench=(docker run --rm --network host redis:7.4-alpine redis-benchmark)
fi

for run in $(seq "$runs"); do
	"${bench[@]}" -p "$port" -n "$requests" -c "$clients" -P "$pipeline" -t "$tests" --csv |
		tail -n +2 | sed "s/^/$run,/"
done
