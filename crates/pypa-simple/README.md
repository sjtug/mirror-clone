# pypa-simple

`pypa-simple` is a vendor-neutral parser and renderer for the PyPA Simple
Repository API. It accepts PEP 503 HTML and PEP 691 JSON and emits matching HTML
and JSON views from one model. Generated project JSON advertises API 1.1 only
when the source supplies PEP 700 `versions` and a `size` for every file;
otherwise it emits a complete API 1.0 representation.

The mirror-clone `simple-repository` source supplies crawling, URL-prefix
rewriting, and storage. It follows nested repository pages by content, so both ordinary
one-level repositories and channel trees work.

Examples (global target options precede the source name):

```sh
# PyTorch: mixed project/channel HTML tree
mirror-clone --target-type s3 --s3-prefix pytorch-wheels \
  simple-repository --index-base https://download.pytorch.org/whl \
  --rewrite-url-prefix https://download-r2.pytorch.org/whl/=/pytorch-wheels/ \
  --rewrite-url-prefix https://download.pytorch.org/whl/=/pytorch-wheels/ \
  --rewrite-url-prefix https://files.pythonhosted.org/packages/=/pypi-packages/ \
  --rewrite-url-prefix https://pypi.nvidia.com/=https://pypi.nvidia.cn/

# Astral: JSON-only channel indexes below an HTML channel root
mirror-clone --target-type s3 --s3-prefix astral-wheels \
  simple-repository --index-base https://wheels.astral.sh/simple/ \
  --rewrite-url-prefix https://wheels.astral.sh/artifacts/=/astral-wheels/artifacts/

# NVIDIA: HTML root/project pages with relative artifact links
mirror-clone --target-type s3 --s3-prefix nvidia-pypi \
  simple-repository --index-base https://pypi.nvidia.cn \
  --rewrite-url-prefix https://pypi.nvidia.cn/=/nvidia-pypi/
```

For S3 targets, mirror-clone appends `/simple` to the configured target prefix.
This keeps generated, deletable indexes separate from on-demand artifact keys.
The serving side mounts them with mirror-intel's generalized
`pypi_index_scope(public_route, index_storage, artifact_storage, ...)` adapter.
