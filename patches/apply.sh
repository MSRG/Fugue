#!/bin/sh
#
# Apply the Fugue patch set to an Apache Flink 1.18.0 source checkout.
#
# Usage: patches/apply.sh <path-to-flink-1.18.0-checkout>
#
# Patches in patches/flink/*.patch are applied with `git apply` in sorted filename order. Every
# patch is verified to apply cleanly before any is applied. Patches are NOT idempotent; reset the
# checkout with `git -C <flink-dir> checkout .` before re-applying.
set -e

PATCH_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
FLINK_DIR=${1:-}

if test -z "$FLINK_DIR"; then
    printf 'usage: %s <path-to-flink-1.18.0-checkout>\n' "$0" >&2
    exit 2
fi
if test ! -f "$FLINK_DIR/pom.xml"; then
    printf 'error: "%s" is not a Flink source checkout (no pom.xml)\n' "$FLINK_DIR" >&2
    exit 1
fi

# Verify all patches apply cleanly first (fail fast, before mutating the checkout).
for patch in "$PATCH_DIR"/flink/*.patch; do
    test -e "$patch" || continue
    if ! git -C "$FLINK_DIR" apply --check "$patch"; then
        printf 'error: patch does not apply cleanly: %s\n' "$patch" >&2
        exit 1
    fi
done

applied=0
for patch in "$PATCH_DIR"/flink/*.patch; do
    test -e "$patch" || continue
    git -C "$FLINK_DIR" apply "$patch"
    printf 'applied %s\n' "${patch##*/}"
    applied=$((applied + 1))
done

printf '%s: %d patch(es) applied to %s\n' "$(basename "$0")" "$applied" "$FLINK_DIR"
