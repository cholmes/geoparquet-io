# publish upload Command

Upload files or directories to cloud object storage.

For detailed usage and examples, see the [Uploading Guide](../guide/upload.md).

## Quick Reference

```bash
gpio publish upload --help
```

## Usage

```bash
gpio publish upload [OPTIONS] SOURCE DESTINATION
```

> **Note:** `--s3-endpoint`, `--s3-region`, `--s3-no-ssl`, and `--aws-profile` are now global options on `gpio`. They still work as command-level flags for backwards compatibility.

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `--aws-profile TEXT` | AWS profile name for S3 operations | - |
| `--pattern TEXT` | Glob pattern for filtering files | - |
| `--max-files INTEGER` | Max parallel file uploads for directories | 4 |
| `--chunk-concurrency INTEGER` | Max concurrent chunks per file | 12 |
| `--chunk-size INTEGER` | Chunk size in bytes for multipart uploads | - |
| `--fail-fast` | Stop immediately on first error | - |
| `--s3-endpoint TEXT` | Custom S3-compatible endpoint | - |
| `--s3-region TEXT` | S3 region | us-east-1 (with custom endpoint) |
| `--s3-no-ssl` | Disable SSL for S3 endpoint | - |
| `-v, --verbose` | Print verbose output | - |
| `--dry-run` | Preview without uploading | - |
| `--help` | Show help message | - |

## Examples

```bash
# Single file to S3
gpio publish upload data.parquet s3://bucket/path/data.parquet

# With AWS profile
gpio publish upload data.parquet s3://bucket/data.parquet --aws-profile source-coop

# Directory upload (preserves structure)
gpio publish upload output/ s3://bucket/dataset/

# Only parquet files
gpio publish upload output/ s3://bucket/dataset/ --pattern "*.parquet"

# Increased parallelism
gpio publish upload output/ s3://bucket/dataset/ --max-files 8

# Stop on first error
gpio publish upload output/ s3://bucket/dataset/ --fail-fast

# S3-compatible endpoint (MinIO)
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint minio.example.com:9000

# Local development (no SSL)
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint localhost:9000 \
  --s3-no-ssl

# Dry run preview
gpio publish upload output/ s3://bucket/dataset/ --dry-run
```

## Supported Destinations

| Protocol | URL Format |
|----------|------------|
| Amazon S3 | `s3://bucket/path/` |
| Google Cloud Storage | `gs://bucket/path/` |
| Azure Blob Storage | `az://account/container/path/` |
| HTTP/HTTPS | `http://` or `https://` |
