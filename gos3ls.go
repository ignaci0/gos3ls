package gos3ls

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	gos3ls_s3 "gos3ls/s3"
)

var (
	bucket   string
	prefix   string
	profile  string
	region   string
	maxCount int	
	outputFilePattern string
	workers int
)

var rootCmd = &cobra.Command{
	Use:   "gos3ls",
	Short: "gos3ls is a CLI tool to interact with S3 bucket objects",
	Long:  `A CLI tool to count and batch S3 objects.`, 
}

func Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called to release resources

	// Set up signal handling
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		<-signals // Wait for a signal
		log.Println("Received termination signal, initiating graceful shutdown...")
		cancel() // Cancel the context
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "f", "", "AWS profile")
	rootCmd.PersistentFlags().StringVarP(&region, "region", "r", "eu-central-1", "AWS region (e.g., us-east-1)")

	rootCmd.AddCommand(countCmd)
	countCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "S3 bucket name")
	countCmd.Flags().StringVarP(&prefix, "prefix", "p", "", "S3 prefix")
	countCmd.MarkFlagRequired("bucket")

	rootCmd.AddCommand(batchCmd)
	batchCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "S3 bucket name")
	batchCmd.Flags().StringVarP(&prefix, "prefix", "p", "", "S3 prefix")
	batchCmd.Flags().IntVarP(&maxCount, "max-count", "m", 1000, "Maximum number of objects per batch")
	batchCmd.Flags().StringVarP(&outputFilePattern, "output-file-pattern", "o", "batch-{{.BatchNum}}.txt", "Output filename pattern for batches. Placeholder: {{.BatchNum}}")
	batchCmd.Flags().IntVarP(&workers, "workers", "w", runtime.NumCPU()*2, "Number of concurrent workers for scanning partitions")
	batchCmd.MarkFlagRequired("bucket")
}

var countCmd = &cobra.Command{
	Use:   "count",
	Short: "Count objects in an S3 bucket",
	Long:  `Count objects in an S3 bucket with an optional prefix.`, 
	Example: `gos3ls count -b my-bucket -p my-prefix/`,
	Run: func(cmd *cobra.Command, args []string) {
		// defaultWorkers := runtime.NumCPU() * 10 // Removed as workers flag is now used
		// workers := defaultWorkers // Using defaultWorkers directly, as flag.Int is removed

		if bucket == "" {
			log.Fatalf("Error: bucket name is required. Use --bucket or -b flag.")
		}

		log.Printf("Starting scan for s3://%s/%s with %d workers...", bucket, prefix, workers)

		// Load AWS configuration
		ctx := context.Background()

		loadOptions := []func(*config.LoadOptions) error{}
		if region != "" {
			loadOptions = append(loadOptions, config.WithRegion(region))
		}

		cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			log.Fatalf("Unable to load AWS SDK config: %v", err)
		}

		// Create the S3 client and the counter instance
		client := s3.NewFromConfig(cfg)
		counter := NewS3Counter(client, bucket, workers)

		// Run the counter and print the result
		count, err := counter.CountObjects(ctx, prefix)
		if err != nil {
			log.Fatalf("Failed to count objects: %v", err)
		}

		log.Printf("Scan complete.")
		fmt.Printf("\nTotal objects found in s3://%s/%s: %d\n", bucket, prefix, count)
	},
}

var batchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Batch objects in an S3 bucket",
	Long:  `Batch objects in an S3 bucket by their leaf name, using goroutines for processing.`, 
	Example: `gos3ls batch -b my-source-bucket -p my-data/ -m 500 -o "batch-{{.BatchNum}}.txt"`,
	Run: func(cmd *cobra.Command, args []string) {
		if bucket == "" {
			log.Fatalf("Error: bucket name is required. Use --bucket or -b flag.")
		}

		log.Printf("Starting batching for s3://%s/%s with max-count %d and %d workers...", bucket, prefix, maxCount, workers)

		ctx := context.Background()

		loadOptions := []func(*config.LoadOptions) error{}
		if region != "" {
			loadOptions = append(loadOptions, config.WithRegion(region))
		}

		cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			log.Fatalf("Unable to load AWS SDK config: %v", err)
		}

		client := s3.NewFromConfig(cfg)

		err = gos3ls_s3.BatchObjects(ctx, client, bucket, prefix, maxCount, outputFilePattern, workers)
		if err != nil {
			log.Fatalf("Failed to batch objects: %v", err)
		}

		log.Printf("Batching complete.")
	},
}

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
