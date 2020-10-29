# mirror-clone

An all-in-one mirror utility

## Overlay Filesystem

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
