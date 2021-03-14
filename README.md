# mirror-clone

An all-in-one mirror utility.

For more information about our mirror service, refer to
[https://github.com/sjtug/mirror-docker-unified/wiki](https://github.com/sjtug/mirror-docker-unified/wiki)
For legacy version, refer to legacy branch.

mirror-clone is originally built for setting up a mirror repo on SJTU S3 service, and now being made
into an all-in-one mirror tool. The most notable feature of mirror-clone is to synchronize files from
Rsync endpoint to S3.

## Design

https://github.com/sjtug/mirror-clone/issues/14

mirror-clone revolves around abstractions like source, target, snapshot, and transfer.
By assembling them, we can get a task that transfers one repo to another.

## Quick Start

```
RUST_LOG=info ./mirror-clone --progress --target-type s3 --s3-prefix homebrew-bottles --s3-buffer-path /srv/disk1/mirror-clone-cache homebrew
RUST_LOG=info ./mirror-clone --progress --target-type s3 --s3-prefix crates.io/crates --s3-buffer-path /srv/disk1/mirror-clone-cache crates-io
RUST_LOG=info ./mirror-clone --progress --target-type file --file-base-path ~/mirror-clone/crates.io --file-buffer-path ~/Work/intel_temp crates-io
```

When running on server, we recommend using `RUST_LOG=info` flag and remove `--progress` flag.

For more usage, refer to our [infra wiki](https://github.com/sjtug/mirror-docker-unified/wiki/Bootstrap-mirror-from-SJTUG).

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
* File system

## Commands

Use `./mirror-clone --help` to view current commands and their meaning.

## Future Works

* More pipes. For example, snapshot cache pipe, etc.
* More sources.
* More transfers.
