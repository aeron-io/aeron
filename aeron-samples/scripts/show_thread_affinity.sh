#!/usr/bin/env bash

set -euo pipefail

pname=${1:-aeronmd}  # default to 'aeronmd'

pid=$(pgrep "${pname}")

echo "PID: ${pid} (${pname})"
ps -T -o pid,tid,comm,psr -p "${pid}"
