# Fugue

Implementation of *Fugue: Online Elasticity for Distributed Stateful Stream
Processing* on Apache Flink 1.18.

## Requirements

- JDK 17
- Maven 3.8+

## Build and test

```bash
mvn -o verify
```

Compiles the modules and runs the unit and integration tests against Apache Flink 1.18.0.

## Run the example

```bash
java -jar fugue-examples/target/fugue-examples-1.0-SNAPSHOT.jar
```

Runs a keyed RocksDB job on an embedded cluster and migrates one key-group's live state between
operator instances.

## Build and run Fugue with Flink

The in-band migration barrier and the atomic cutover need a small patch set applied to Flink itself.
Build a Fugue-patched Flink 1.18.0 once:

```bash
git clone --depth 1 --branch release-1.18.0 https://github.com/apache/flink.git ../flink-1.18.0
./build-flink.sh ../flink-1.18.0
```

Run the full suite against it:

```bash
mvn -o verify -P patched
```

To run a Fugue job on a standalone Flink cluster, build a distribution from the patched checkout and
submit an example job:

```bash
# Build a Flink distribution that bundles the patched runtime
( cd ../flink-1.18.0 && mvn install -DskipTests -pl flink-dist -am \
    -Dspotless.check.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Denforcer.skip=true )

# Build the Fugue jars, start a local cluster, and submit the job
mvn -o package
FLINK=../flink-1.18.0/flink-dist/target/flink-1.18.0-bin/flink-1.18.0
"$FLINK"/bin/start-cluster.sh
"$FLINK"/bin/flink run -c org.apache.flink.fugue.examples.WordCountWithFugue \
    fugue-examples/target/fugue-examples-1.0-SNAPSHOT.jar
```

`build-flink.sh` installs patched `flink-runtime` and `flink-streaming-java` 1.18.0 into `~/.m2`. To revert:

```bash
rm -rf ~/.m2/repository/org/apache/flink/{flink-runtime,flink-streaming-java}/1.18.0
```
