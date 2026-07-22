#!/bin/sh
#
# Build a Fugue-patched Apache Flink 1.18.0 into the local Maven repository (~/.m2), so the Fugue
# modules (which depend on Flink 1.18.0) resolve the patched runtime instead of Maven Central's.
#
# Usage: ./build-flink.sh <path-to-flink-1.18.0-checkout>
#
# Override the Flink modules to build via FUGUE_FLINK_MODULES (comma-separated):
#   FUGUE_FLINK_MODULES="flink-runtime,flink-streaming-java" ./build-flink.sh /path/to/flink-1.18.0
#
# Flink 1.18 builds on JDK 17 via its auto-activated `java17` Maven profile.
set -e

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
FLINK_DIR=${1:-}
MODULES=${FUGUE_FLINK_MODULES:-flink-runtime,flink-streaming-java}

if test -z "$FLINK_DIR"; then
    printf 'usage: %s <path-to-flink-1.18.0-checkout>\n' "$0" >&2
    exit 2
fi

# Flink 1.18 must be built with JDK 8/11/17; the host default JDK may be newer. Prefer JDK 17.
if test -z "${JAVA_HOME:-}"; then
    JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    export JAVA_HOME
fi
printf 'JAVA_HOME=%s\n' "${JAVA_HOME:-<inherited>}"

# Vendor the Fugue sources into flink-runtime so the patched JobMaster/TaskExecutor can reference
# them (Fugue depends on flink-runtime, so it must live inside it to avoid a dependency cycle).
FUGUE_PKG="$FLINK_DIR/flink-runtime/src/main/java/org/apache/flink/fugue"
printf 'vendoring Fugue sources into %s\n' "$FUGUE_PKG"
rm -rf "$FUGUE_PKG"
mkdir -p "$FUGUE_PKG"
for module in fugue-common fugue-coordinator fugue-runtime fugue-integration; do
    cp -R "$SCRIPT_DIR/$module/src/main/java/org/apache/flink/fugue/." "$FUGUE_PKG/"
done

# Drop Fugue classes that depend on Maven modules higher than flink-runtime (flink-streaming-java /
# flink-statebackend-rocksdb): they cannot compile inside the vendored flink-runtime, and are
# exercised only by the repo's operator-level state-transfer tests,
# never by the in-runtime control plane / barrier plumbing.
rm -f "$FUGUE_PKG/runtime/transfer/OperatorStateMigrationService.java"
rm -f "$FUGUE_PKG/runtime/transfer/NetworkedKeyGroupTransfer.java"
rm -f "$FUGUE_PKG/runtime/transfer/NetworkedStateMigrationService.java"
rm -rf "$FUGUE_PKG/runtime/operator"

# Reset previously-patched files so the (non-idempotent) patch set re-applies cleanly on re-runs.
git -C "$FLINK_DIR" checkout -- \
    flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java \
    flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskExecutor.java \
    flink-runtime/pom.xml \
    flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/io/AbstractStreamTaskNetworkInput.java \
    flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java \
    flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/CheckpointedInputGate.java \
    flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/KeyGroupStreamPartitioner.java \
    flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/InternalKeyContext.java \
    flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/InternalKeyContextImpl.java \
    flink-runtime/src/main/java/org/apache/flink/runtime/state/AbstractKeyedStateBackend.java 2>/dev/null || true

sh "$SCRIPT_DIR/patches/apply.sh" "$FLINK_DIR"

printf 'building patched Flink modules [%s] into ~/.m2 (first build can take >10 min)...\n' "$MODULES"
# Skip Flink's source-formatting/license checks: the vendored Fugue sources are not formatted to
# Flink's spotless/checkstyle rules and would otherwise fail the build.
( cd "$FLINK_DIR" && mvn -B -pl "$MODULES" -am -DskipTests \
    -Dspotless.check.skip=true -Dcheckstyle.skip=true -Drat.skip=true \
    -Denforcer.skip=true -Dmaven.javadoc.skip=true install )
printf 'done: Fugue now resolves the patched Flink 1.18.0 from ~/.m2\n'
