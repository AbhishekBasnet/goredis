#!/usr/bin/env bash
set -euo pipefail

goredis_port=${GOREDIS_PORT:-6380}
redis_port=${REDIS_PORT:-6390}
commands=${1:-test/compat/commands.txt}

if command -v redis-cli >/dev/null; then
	cli=(redis-cli)
else
	cli=(docker run --rm -i --network host redis:7.4-alpine redis-cli)
fi

run() {
	"${cli[@]}" -p "$1" <"$commands"
}

diff -u --label "redis :$redis_port" --label "goredis :$goredis_port" \
	<(run "$redis_port") <(run "$goredis_port") && echo "identical: $(grep -c . "$commands") commands"
