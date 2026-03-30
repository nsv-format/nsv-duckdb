#!/bin/bash
# Wrapper that bypasses wasm-opt entirely.
# wasm-opt aggressively strips Rust staticlib symbols from the function
# table, causing "null function or function signature mismatch" traps
# at runtime. Skipping it keeps all symbols intact.

if [ "$1" = "--version" ] || [ "$1" = "-version" ]; then
    echo "wasm-opt version 124 (wrapper - optimization disabled)"
    exit 0
fi

output_file=""
input_file=""
next_is_output=0

for arg in "$@"; do
    if [ "$arg" = "-o" ]; then
        next_is_output=1
    elif [ $next_is_output -eq 1 ]; then
        output_file="$arg"
        next_is_output=0
    elif [ -f "$arg" ] && [ -z "$input_file" ]; then
        input_file="$arg"
    fi
done

if [ -n "$input_file" ] && [ -n "$output_file" ]; then
    cp "$input_file" "$output_file"
    exit 0
fi

exit 0
