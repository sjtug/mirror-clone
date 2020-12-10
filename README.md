# mirror-clone

An all-in-one mirror utility

For legacy version, refer to legacy branch.

## Design

https://github.com/sjtug/mirror-clone/issues/14

mirror-clone revolves around abstractions like source, target, snapshot, and transfer.
By assembling them, we can get a task that transfers one repo to another.

## Implementation

### Transfer

* Simple Diff Transfer, compares filename and transfer what's missing in target.

### Snapshot

* rsync snapshot, gets file list of a rsync repo.
* PyPI snapshot, gets all packages from PyPI simple index.
* rustup snapshot, gets all toolchains from `channel-xxx-xxxx.toml`

### Source

Currently all snapshots are also source. We'll add standalone source later.
### Target

* mirror-intel, sends HEAD request to [mirror-intel](https://github.com/sjtug/mirror-intel) endpoint, so as to fill the mirror-intel cache.

## Commands

Use `./mirror-intel --help` to view current commands and their meaning.
