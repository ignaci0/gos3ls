# gos3ls: S3 CLI Tool

`gos3ls` is a command-line tool written in Go designed to interact with S3 bucket objects. It provides functionalities for counting objects and batching their leaf names into local files, leveraging concurrent processing for efficiency.

## Features

*   **Concurrent Scanning:** Utilizes Go routines to scan multiple S3 partitions (subdirectories) simultaneously for both counting and batching operations.
*   **Efficient S3 Interaction:** Employs the S3 List API with appropriate parameters to efficiently retrieve object information.
*   **Configurable Concurrency:** The number of worker goroutines can be configured to optimize performance based on your environment and S3 structure.
*   **Standard AWS Authentication:** Uses the default AWS SDK credential chain (environment variables, shared credentials file, IAM roles).
*   **Batching of Object Leaf Names:** Extracts the last part of S3 object keys (leaf names), batches them, and writes them to local files.

## Prerequisites

*   Go (version 1.18 or higher)
*   Configured AWS credentials. The easiest way is to have a `~/.aws/config` and `~/.aws/credentials` file, or set the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc., environment variables.

## Installation & Build

1.  Initialize Go modules:
    ```bash
    go mod tidy
    ```
2.  Build the executable:
    ```bash
    CGO_ENABLED=0 go build -o gos3ls .
    ```
    This will create an executable file named `gos3ls` in the current directory.

## Usage

The `gos3ls` tool provides subcommands for different operations. You can get general help or help for specific subcommands.

### General Help

```bash
./gos3ls --help
```

### `count` Command

Counts objects in an S3 bucket with an optional prefix. It uses concurrent workers to speed up the scan.

**Flags:**

*   `--bucket`, `-b <name>`: S3 bucket name (required).
*   `--prefix`, `-p <prefix>`: S3 prefix (optional).
*   `--profile`, `-f <profile>`: AWS profile to use (optional).
*   `--region`, `-r <region>`: AWS region (e.g., `eu-central-1`, default: `eu-central-1`).

**Example:**

```bash
./gos3ls count -b my-awesome-bucket -p data/logs/
```

### `batch` Command

Batches S3 object leaf names into local files. It concurrently scans S3 partitions, extracts leaf names, and writes them to files, with each file containing a specified maximum number of leaf names.

**Flags:**

*   `--bucket`, `-b <name>`: S3 bucket name (required).
*   `--prefix`, `-p <prefix>`: S3 prefix (optional).
*   `--max-count`, `-m <count>`: Maximum number of object leaf names per batch file (default: `1000`).
*   `--output-file-pattern`, `-o <pattern>`: Output filename pattern for batches. Supports placeholder:
    *   `{{.BatchNum}}`: Replaced by the sequential batch number.
    (Default: `batch-{{.BatchNum}}.txt`)
*   `--workers`, `-w <count>`: Number of concurrent workers for scanning S3 partitions (default: `2 * NumCPU`).
*   `--profile`, `-f <profile>`: AWS profile to use (optional).
*   `--region`, `-r <region>`: AWS region (e.g., `eu-central-1`, default: `eu-central-1`).

**Examples:**

Batching leaf names from `my-source-bucket/my-data/` into files named `batch-1.txt`, `batch-2.txt`, etc., with 500 leaf names per file:

```bash
./gos3ls batch -b my-source-bucket -p my-data/ -m 500
```

Batching all leaf names from `another-bucket/` using 10 workers and a custom output pattern:

```bash
./gos3ls batch -b another-bucket -w 10 -o "output-{{.BatchNum}}.txt"
```

### Getting Help for Subcommands

```bash
./gos3ls batch --help
./gos3ls count --help
```