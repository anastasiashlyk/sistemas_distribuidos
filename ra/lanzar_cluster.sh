#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RPIS=("rpi1" "rpi2" "rpi3" "rpi4")
USERS="users_cluster.txt"
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

for rpi in "${RPIS[@]}"; do
    ssh -o ConnectTimeout=2 "$rpi" "pkill -f 'cmd/nodo' || true" 2>/dev/null &
done
wait
sleep 0.2

trap 'for rpi in "${RPIS[@]}"; do ssh "$rpi" "pkill -f \"cmd/nodo\"" 2>/dev/null & done; wait; exit 1' INT TERM

for i in "${!RPIS[@]}"; do
    rpi="${RPIS[$i]}"
    pid=$((i + 1))
    op="${ROLES[$i]}"
    ssh "$rpi" "cd '$DIR' && go run cmd/nodo/main.go -id=$pid -users=$USERS -op=$op -iter=$ITER" 2>&1 | sed "s/^/[$rpi P$pid] /" &
done

wait


