#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

# Run llingr-demux benchmarks (pure Go)
#
# usage:
#   ./benchmarks/run_benchmarks.sh                    # Run default config
#   ./benchmarks/run_benchmarks.sh 35ms               # Run 35ms latency config
#   ./benchmarks/run_benchmarks.sh <path/to/config>   # Run custom config
#
# Results are appended to benchmarks/benchmark_results.csv
# For visualization see: ./benchmarks/generate_html.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

# Determine config file
CONFIG="benchmarks/runner/configs/benchmark_configs.json"
if [[ "$1" == "35ms" ]]; then
    CONFIG="benchmarks/runner/configs/benchmark_configs_35ms.json"
elif [[ -n "$1" ]]; then
    CONFIG="$1"
fi

printf "running benchmarks with config: $CONFIG\n"
printf "output: benchmarks/benchmark_results.csv\n\n"

go run ./benchmarks/runner -config="$CONFIG" -output="benchmarks/benchmark_results.csv"

printf "done, run ./benchmarks/generate_html.sh to create visualization.\n\n"
