# mirror-clone

An all-in-one mirror utility.

For more information about our mirror service, refer to https://github.com/sjtug/mirror-docker-unified/wiki

For legacy version, refer to legacy branch.

## Design

https://github.com/sjtug/mirror-clone/issues/14

mirror-clone revolves around abstractions like source, target, snapshot, and transfer.
By assembling them, we can get a task that transfers one repo to another.

## Quick Start

RUST_LOG=info cargo run --release -- --progress --target-type s3 --s3-prefix homebrew-bottles --s3-buffer-path /tmp homebrew
RUST_LOG=info cargo run --release -- --progress --target-type s3 --s3-prefix crates.io/crates --s3-buffer-path /tmp crates-io

## Implementation

### Transfer

* Simple Diff Transfer, compares filename and transfer what's missing in target.

### Snapshot

Refer to source code for more information.

### Source

Refer to source code for more information.
### Target

* mirror-intel, sends HEAD request to [mirror-intel](https://github.com/sjtug/mirror-intel) endpoint, so as to fill the mirror-intel cache.
* S3

## Commands

Use `./mirror-intel --help` to view current commands and their meaning.
