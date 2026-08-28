# Uploading to Cloud Storage

The `gpio publish upload` command uploads files and directories to cloud object storage, supporting S3, GCS, Azure, and HTTP destinations.

## Quick Start

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Upload single file to S3
    gpio publish upload data.parquet s3://bucket/path/data.parquet

    # Upload directory (preserves structure)
    gpio publish upload output/ s3://bucket/dataset/

    # With AWS profile
    gpio publish upload data.parquet s3://bucket/data.parquet --aws-profile my-profile
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read("data.parquet")
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    # Upload to S3
    table.upload("s3://bucket/path/data.parquet", profile="my-profile")
    ```

## Supported Destinations

| Destination | URL Format | Example |
|-------------|------------|---------|
| Amazon S3 | `s3://` | `s3://my-bucket/path/file.parquet` |
| Google Cloud Storage | `gs://` | `gs://my-bucket/path/file.parquet` |
| Azure Blob Storage | `az://` | `az://container/path/file.parquet` |
| HTTP/HTTPS | `http://` or `https://` | `https://api.example.com/upload` |

## Directory Uploads

When uploading directories, gpio preserves the directory structure and uploads files in parallel:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Upload all files
gpio publish upload output/ s3://bucket/dataset/

# Only parquet files
gpio publish upload output/ s3://bucket/dataset/ --pattern "*.parquet"

# Increase parallelism
gpio publish upload output/ s3://bucket/dataset/ --max-files 8
```

## AWS Configuration

### Using AWS Profiles

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet --aws-profile source-coop
```

### S3-Compatible Endpoints

For MinIO, Wasabi, or other S3-compatible storage:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint minio.example.com:9000 \
  --s3-region us-east-1
```

### Disable SSL

For local development or non-SSL endpoints:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint localhost:9000 \
  --s3-no-ssl
```

## Multipart Uploads

Large files are automatically uploaded using multipart uploads:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Customize chunk settings
gpio publish upload large.parquet s3://bucket/large.parquet \
  --chunk-size 104857600 \
  --chunk-concurrency 12
```

## Error Handling

By default, directory uploads continue on errors. Use `--fail-fast` to stop on first error:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload output/ s3://bucket/dataset/ --fail-fast
```

## Dry Run

Preview what would be uploaded without actually uploading:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload output/ s3://bucket/dataset/ --dry-run
```

## CLI Reference

See the [CLI Reference](../cli/upload.md) for complete options.
