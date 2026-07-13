#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

# Generate benchmark visualization HTML from CSV data.
# Uses Docker - no Python installation required.
#
# Usage:
#   ./benchmarks/generate_html.sh
#
# Input:  benchmarks/benchmark_results.csv
# Output: benchmarks/llingr-demux-benchmarks.html

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_IMAGE="python:3.12-alpine"

cd "$REPO_ROOT"

echo "Generating charts via Docker ($PYTHON_IMAGE)..."

# Capture host info (not available inside container)
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "")
HOST_OS=$(grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d'"' -f2 || echo "")
HOST_KERNEL=$(uname -r 2>/dev/null || echo "")
HOST_CPU=$(lscpu 2>/dev/null | grep 'Model name' | cut -d':' -f2 | xargs || echo "")
HOST_CORES=$(lscpu 2>/dev/null | grep '^CPU(s):' | cut -d':' -f2 | xargs || echo "")
HOST_MEMORY=$(free -h 2>/dev/null | awk '/^Mem:/ {print $2}' || echo "")
HOST_ARCH=$(uname -m 2>/dev/null || echo "")

docker run --rm \
    -v "$SCRIPT_DIR:/benchmarks" \
    -w /benchmarks \
    --user "$(id -u):$(id -g)" \
    -e "GIT_COMMIT=$GIT_COMMIT" \
    -e "HOST_OS=$HOST_OS" \
    -e "HOST_KERNEL=$HOST_KERNEL" \
    -e "HOST_CPU=$HOST_CPU" \
    -e "HOST_CORES=$HOST_CORES" \
    -e "HOST_MEMORY=$HOST_MEMORY" \
    -e "HOST_ARCH=$HOST_ARCH" \
    "$PYTHON_IMAGE" \
    python3 generate_chart.py

echo "Done. Open benchmarks/llingr-demux-benchmarks.html to view results."
