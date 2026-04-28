#!/bin/bash
################################################################################
#
#    Copyright (c) 2026.
#    Haixing Hu, Qubit Co. Ltd.
#
#    All rights reserved.
#
################################################################################
#
# Enforce per-file coverage thresholds on cargo-llvm-cov text output.
#

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <coverage-text-report>" >&2
    exit 2
fi

coverage_log="$1"
if [ ! -f "$coverage_log" ]; then
    echo "Coverage report not found: $coverage_log" >&2
    exit 2
fi

awk '
    function percent(value) {
        gsub(/%/, "", value)
        return value + 0
    }
    function report(file, metric, value, rule) {
        printf "Coverage threshold failed: %s %s coverage is %.2f%% (%s)\n", file, metric, value, rule > "/dev/stderr"
    }
    BEGIN {
        failed = 0
        in_table = 0
    }
    /^Filename[[:space:]]/ {
        in_table = 1
        next
    }
    !in_table || /^-+$/ || /^TOTAL[[:space:]]/ || NF < 10 {
        next
    }
    {
        file = $1
        region_coverage = percent($4)
        function_coverage = percent($7)
        line_coverage = percent($10)
        if (function_coverage < 100) {
            report(file, "function", function_coverage, "required 100%")
            failed = 1
        }
        if (line_coverage <= 98) {
            report(file, "line", line_coverage, "required > 98%")
            failed = 1
        }
        if (region_coverage <= 98) {
            report(file, "region", region_coverage, "required > 98%")
            failed = 1
        }
    }
    END {
        exit failed
    }
' "$coverage_log"
