# S3 Concurrent Object Counter

This is a command-line tool written in Go that recursively counts the total number of objects within a given S3 prefix. It uses a pool of concurrent goroutines to scan different parts of the prefix simultaneously, making it significantly faster than a simple serial scan for prefixes with many objects and subdirectories.
Features

    Concurrent Scanning: Utilizes Go routines to scan multiple S3 "directories" at once.

    Efficient: Uses the Delimiter parameter in the S3 List API to avoid listing all keys at once.

    Configurable Concurrency: The number of worker goroutines can be easily configured.

    Standard AWS Authentication: Uses the default AWS SDK credential chain (environment variables, shared credentials file, IAM roles).

# Prerequisites

    Go (version 1.18 or higher)

    Configured AWS credentials. The easiest way is to have a ~/.aws/config and ~/.aws/credentials file, or set the standard AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc., environment variables.

# Installation & Build

    go mod tidy

    Build the executable:

    CGO_ENABLED=0 go build .

    This will create an executable file named s3-counter in the current directory.

# Usage

Run the compiled binary, passing the S3 URI of the prefix you want to scan as the only argument.
Basic Example

./s3-counter s3://my-awesome-bucket/data/

Example with a deeper prefix

./s3-counter s3://my-awesome-bucket/logs/production/2023/

Customizing Worker Count

You can specify the number of concurrent workers using the -workers flag. The default is 10 times the number of CPU cores. Increasing this number can speed up scans on very large and deep prefixes, but may also increase API costs and memory usage.

# Use 200 concurrent workers
./s3-counter -workers 200 s3://my-large-dataset-bucket/raw-files/

Getting Help

./s3-counter --help


