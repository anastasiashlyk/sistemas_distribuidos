#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USERS="$DIR/users_localhost.txt"
ROLES=("R" "R" "W" "W")
ITER=5

case "$1" in
    T1) ROLES=("R" "R" "R" "R") ;;
    T2) ROLES=("W" "W" "W" "W") ;;
    T3) ROLES=("R" "R" "W" "W") ;;
    R|W)
        ROLES=("$1" "${2:-R}" "${3:-W}" "${4:-W}")
        ITER="${5:-$ITER}"
        ;;
    [0-9]*) ITER="$1" ;;
esac

if [[ "$2" =~ ^[0-9]+$ ]]; then
    ITER="$2"
fi

pkill -f 'cmd/nodo' 2>/dev/null || true
trap 'pkill -f "cmd/nodo" 2>/dev/null || true; exit 1' INT TERM

for i in 1 2 3 4; do
    idx=$((i - 1))
    op="${ROLES[$idx]}"
    go run "$DIR/cmd/nodo/main.go" -id=$i -users="$USERS" -op=$op -iter=$ITER 2>&1 | sed "s/^/[P$i] /" &
done

wait


