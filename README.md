# mirror-clone

An all-in-one mirror utility

## Example

```bash
cargo run -- conda https://mirrors.sjtug.sjtu.edu.cn/anaconda/pkgs/main/win-64 /srv/data/conda/pkgs/main/win-64
```

## Motivation

The observation is that, software registeries follow nearly the same design. They
have an index to store all packages, they use checksums to verify if package is
corrupted. And for mirror utility developers, they also share a lot of components
when building a mirroring tool. They need to extract tarballs, parse configs,
support multi-thread downloads, verify integrity, etc.

mirror-clone is built to simplify how we build a mirror utility. By providing
a large set of reuseble functions, developers could port mirror-clone to a new
mirror very fast. By using the Rust programming language, we could also mirror
software registery faster, while making the utility robust.

## Roadmap

- Repo
  - [x] OPAM support
  - [x] Conda support
  - [ ] rust-static support
  - [ ] static.crates.io support
  - [ ] apt support
- Functionalities
  - [x] Download
  - [x] Concurrent control
  - [ ] Multi-thread executor
  - [ ] Easy-to-use macro and interface
  - [x] Checksum verification
  - [x] HTTP error handling
  - [ ] Multi-thread checksum

## Features

### Overlay Filesystem

The basis of mirror-clone is a virtual "overlay filesystem", that maintains a working
"snapshot" of software registery on disk.

All files downloaded could be tracked by the "overlay filesystem".
The filesystem will automatically resolve dependencies between files
and remove stale files.

For example, in Ubuntu, `Packages*`, `InRelease`, etc. should be updated
after all packages have been mirrored. This way, we could construct something
like this:

```rust
let mut in_release = base.create_file_for_write("InRelease")).await?;
download_to_file("InRelease", &mut in_release).await?; // First, download `InRelease` to overlay fs
// Then, download other packages
let mut pkg = base.create_file_for_write("somepackage")).await?;
download_to_file("somepackage", &mut pkg).await?; // First, download `InRelease` to overlay fs
pkg.commit().await?;
// ...
// ...
// ...
// Finally, commit `InRelease`
in_release.commit().await?;
```

When file is being downloaded, it will be written to `<filename>.<rand>.tmp`.
When committing file, it will be renamed to `<filename>`. When overlay file is dropped
in Rust, it will be removed.

### Unified Logging Interface

All tasks in mirror-clone use the same logging interface. Logs could be easily parsed
and processed by existing tools.

### Managed Download

**Auto Retry.** It's normal that packages could not be downloaded because of network error.
mirror-clone automatically handles HTTP errors and retries the requests.

**Auto Pipelining.** mirror-clone automatically scales number of concurrent downloads.
