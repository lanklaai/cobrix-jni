# cobrix-jni

This repository provides a thin JNI bridge so Rust can use Cobrix copybook parsing/decoding without Spark.

## What this adds

- **Scala bridge (`CobrixBridge`)**
  - Reads a copybook and returns a JSON schema summary.
  - Opens a streaming reader and returns records in **batches** as JSON strings.
  - Supports record-by-record reading by setting `batch_size = 1`.
- **Rust API (`cobrix-jni`)**
  - Starts an embedded JVM.
  - Calls into the Scala bridge via JNI.
  - Exposes `schema_from_copybook()` and `open_batch_reader().next_batch()`.

## Build

1. Build the Scala bridge JAR (includes Cobrix dependencies):
   ```bash
   sbt assembly
   ```
   > Requires [sbt](https://www.scala-sbt.org/download/) 1.10+ on your `PATH`.
2. Build the Rust crate:
   ```bash
   cargo build
   ```
3. Use the generated shaded JAR in Rust JVM classpath.

## Notes

- This implementation intentionally avoids Spark APIs.
- Decoding is done via Cobrix parser classes and reflection to keep the wrapper resilient across Cobrix versions.
