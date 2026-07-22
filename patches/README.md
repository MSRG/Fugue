# Fugue → Flink patch set

Fugue integrates with Apache Flink by (a) the Fugue Maven modules in this repo and (b) a small set
of patches to Flink's own runtime applied to a **separate** Flink 1.18.0 source checkout. The Flink
checkout is *not* committed to this repo.

## Layout

- `patches/flink/*.patch` — unified diffs applied to the Flink checkout with `git apply`, in sorted
  filename order (prefix with `NN-` to order them).
- `patches/apply.sh <flink-dir>` — verifies every patch applies cleanly, then applies them.
- `../build-flink.sh <flink-dir>` — applies the patch set and builds the patched Flink modules into
  the local Maven repo (`~/.m2`), so the Fugue modules (which depend on Flink `1.18.0`) resolve the
  *patched* runtime instead of the one from Maven Central.

## One-time setup

```sh
git clone --depth 1 --branch release-1.18.0 https://github.com/apache/flink.git /path/to/flink-1.18.0
```

## Apply + build

```sh
# Flink 1.18 builds on JDK 17 (auto-activates its `java17` Maven profile); the repo's modules also
# build/test on JDK 17.
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

./build-flink.sh /path/to/flink-1.18.0
# then, from the repo root:
mvn -DskipTests install      # Fugue modules against the patched Flink
mvn verify                   # runs the MiniCluster *ITCase tests
```

To reset the Flink checkout before re-applying (patches are not idempotent):

```sh
git -C /path/to/flink-1.18.0 checkout .
```

## Modules built

`build-flink.sh` builds `flink-runtime` by default (with `-am`, i.e. its upstream dependencies).
Override via `FUGUE_FLINK_MODULES` (comma-separated) once patches touch more modules, e.g.:

```sh
FUGUE_FLINK_MODULES="flink-runtime,flink-streaming-java" ./build-flink.sh /path/to/flink-1.18.0
```
