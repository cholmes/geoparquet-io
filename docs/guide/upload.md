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
| Azure Blob Storage | `az://` | `az://myaccount/mycontainer/path/file.parquet` |
| HTTP/HTTPS | `http://` or `https://` | `https://api.example.com/upload` |

### Azure URLs name the account first

An Azure destination is `az://<account>/<container>/<path>` — the storage account, then the container, then the key. gpio builds the store from those two segments itself, so the account never has to be in the environment and is never mistaken for the container ([#864](https://github.com/geoparquet/geoparquet-io/issues/864)).

The credential still comes from the environment, and gpio checks for one before it uploads:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Storage account key
export AZURE_STORAGE_ACCOUNT_KEY=your_key

# ...or a SAS token
export AZURE_STORAGE_SAS_TOKEN=your_token

gpio publish upload data.parquet az://myaccount/mycontainer/data.parquet
```

`AZURE_STORAGE_ACCESS_KEY`, `AZURE_STORAGE_SAS_KEY`, the `AZURE_STORAGE_CLIENT_*` client-secret variables and `AZURE_USE_AZURE_CLI` are honoured too. `AZURE_STORAGE_ACCOUNT_NAME` is not needed — the account in the URL wins over it.

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
