package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Counter holds the state for the counting operation.
type S3Counter struct {
	client     *s3.Client
	bucket     string
	totalCount int64
	numWorkers int
}

// NewS3Counter initializes the counter.
func NewS3Counter(client *s3.Client, bucket string, workers int) *S3Counter {
	return &S3Counter{
		client:     client,
		bucket:     bucket,
		numWorkers: workers,
	}
}

// CountObjects starts the concurrent counting process.
func (sc *S3Counter) CountObjects(ctx context.Context, rootPrefix string) (int64, error) {
	var wg sync.WaitGroup
	prefixesToScan := make(chan string, sc.numWorkers*2)

	// Start a pool of worker goroutines
	for i := 0; i < sc.numWorkers; i++ {
		go sc.worker(ctx, &wg, prefixesToScan)
	}

	// Seed the process with the initial root prefix
	wg.Add(1)
	prefixesToScan <- rootPrefix

	// Wait for all scanning to complete, then close the channel
	wg.Wait()
	close(prefixesToScan)

	return sc.totalCount, nil
}

// worker is a goroutine that pulls prefixes from a channel, scans them, and enqueues sub-prefixes.
func (sc *S3Counter) worker(ctx context.Context, wg *sync.WaitGroup, prefixesToScan chan string) {
	for prefix := range prefixesToScan {
		// Scan the prefix to get object counts and a list of new sub-prefixes.
		newPrefixes, err := sc.scanPrefix(ctx, prefix)
		if err != nil {
			// The error is already logged in scanPrefix.
			// We still need to mark the current task as done.
			wg.Done()
			continue
		}

		// For each new sub-prefix found, add it to the waitgroup and the channel.
		for _, newPrefix := range newPrefixes {
			wg.Add(1)
			prefixesToScan <- newPrefix
		}

		// Mark the original prefix as done.
		wg.Done()
	}
}

// scanPrefix lists objects for a given prefix, adds to the total count,
// and returns a slice of any common prefixes ("subdirectories") found.
func (sc *S3Counter) scanPrefix(ctx context.Context, prefix string) ([]string, error) {
	var discoveredPrefixes []string
	paginator := s3.NewListObjectsV2Paginator(sc.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(sc.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("ERROR: Failed to list objects for prefix '%s': %v", prefix, err)
			return nil, err // Return the error to the worker
		}

		// Add the number of objects found on this page to the total count.
		atomic.AddInt64(&sc.totalCount, int64(len(page.Contents)))

		// Collect all "subdirectories" to be returned.
		for _, commonPrefix := range page.CommonPrefixes {
			discoveredPrefixes = append(discoveredPrefixes, *commonPrefix.Prefix)
		}
	}
	return discoveredPrefixes, nil
}

// parseS3URI extracts the bucket and prefix from a standard s3:// URI.
func parseS3URI(uri string) (bucket, prefix string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("could not parse URI: %w", err)
	}

	if parsed.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid scheme: expected 's3', got '%s'", parsed.Scheme)
	}

	if parsed.Host == "" {
		return "", "", fmt.Errorf("invalid URI: missing bucket name")
	}

	bucket = parsed.Host
	prefix = strings.TrimLeft(parsed.Path, "/")
	return bucket, prefix, nil
}

func main() {
	// Define and parse command-line flags
	defaultWorkers := runtime.NumCPU() * 10
	workers := flag.Int("workers", defaultWorkers, "Number of concurrent workers to run.")
	// --- MODIFIED --- Set the default region to eu-central-1
	region := flag.String("region", "eu-central-1", "The AWS region of the bucket. (e.g., us-east-1).")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] s3://bucket/prefix/\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "A tool to recursively count objects in an S3 prefix using concurrent workers.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	s3URI := flag.Arg(0)

	// Parse the S3 URI
	bucket, prefix, err := parseS3URI(s3URI)
	if err != nil {
		log.Fatalf("Invalid S3 URI: %v", err)
	}

	log.Printf("Starting scan for %s with %d workers...", s3URI, *workers)

	// Load AWS configuration
	ctx := context.Background()

	// --- MODIFIED --- Build config options, adding region if provided
	loadOptions := []func(*config.LoadOptions) error{}
	if *region != "" {
		loadOptions = append(loadOptions, config.WithRegion(*region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		log.Fatalf("Unable to load AWS SDK config: %v", err)
	}

	// Create the S3 client and the counter instance
	client := s3.NewFromConfig(cfg)
	counter := NewS3Counter(client, bucket, *workers)

	// Run the counter and print the result
	count, err := counter.CountObjects(ctx, prefix)
	if err != nil {
		log.Fatalf("Failed to count objects: %v", err)
	}

	log.Printf("Scan complete.")
	fmt.Printf("\nTotal objects found in %s: %d\n", s3URI, count)
}
