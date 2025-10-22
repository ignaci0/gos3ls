package s3

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// BatchObjects lists objects in an S3 bucket, extracts their leaf names,
// and writes them to batched output files concurrently.
func BatchObjects(ctx context.Context, client *s3.Client, bucket, rootPrefix string, maxCount int, outputFilePattern string, numWorkers int) error {
	// Channel to send partition prefixes to scanner goroutines
	partitionsChan := make(chan string)
	// Channel to send leaf names from scanner goroutines to the writer goroutine
	leafNamesChan := make(chan string)

	var wgScanners sync.WaitGroup
	var wgWriter sync.WaitGroup

	log.Printf("Starting batching for s3://%s/%s with %d scanner workers...", bucket, rootPrefix, numWorkers)

	// 1. Start scanner goroutines
	for i := 0; i < numWorkers; i++ {
		wgScanners.Add(1)
		go func() {
			defer wgScanners.Done()
			for partitionPrefix := range partitionsChan {
				log.Printf("Scanning partition: %s", partitionPrefix)
				paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
					Prefix: aws.String(partitionPrefix),
				})

				for paginator.HasMorePages() {
					page, err := paginator.NextPage(ctx)
					if err != nil {
						log.Printf("ERROR: Failed to list objects for prefix %s: %v", partitionPrefix, err)
						// Continue to next page/partition, don't stop the whole process
						continue
					}

					for _, obj := range page.Contents {
						if obj.Key != nil {
							parts := strings.Split(*obj.Key, "/")
							leafName := parts[len(parts)-1]
							if leafName != "" { // Avoid adding empty strings if key ends with '/' 
								leafNamesChan <- leafName
							}
						}
					}
				}
			}
		}()
	}

		    		// Determine a base leaf name for the output file pattern.

		    		// This is used for the {{.LeafName}} placeholder in the output file pattern.

		    		// It defaults to "batch" if no meaningful leaf name can be derived from the rootPrefix.

		    		outputLeafName := "batch"

		    		if rootPrefix != "" {

		    			trimmedPrefix := strings.TrimSuffix(rootPrefix, "/")

		    			parts := strings.Split(trimmedPrefix, "/")

		    			if len(parts) > 0 && parts[len(parts)-1] != "" {

		    				outputLeafName = parts[len(parts)-1]

		    			}

		    		}

		    	

		    		// 2. Start writer goroutine

		    		wgWriter.Add(1)

		    		go func(outputLeafName string) {

		    			defer wgWriter.Done()

		    	

		    			var currentBatch []string

		    			batchNum := 1

		    	

		    			writeBatchToFile := func() error {

		    				if len(currentBatch) == 0 {

		    					return nil

		    				}

		    	

		    				data := struct {

		    					LeafName string

		    					BatchNum int

		    				}{

		    					LeafName: outputLeafName,

		    					BatchNum: batchNum,

		    				}

		

		            tmpl, err := template.New("outputFile").Parse(outputFilePattern)

		            if err != nil {

		                return fmt.Errorf("failed to parse output file pattern: %w", err)

		            }

		

		            var fileNameBuf bytes.Buffer

		            err = tmpl.Execute(&fileNameBuf, data)

		            if err != nil {

		                return fmt.Errorf("failed to execute output file pattern template: %w", err)

		            }

		            outputFileName := fileNameBuf.String()

		

		            file, err := os.Create(outputFileName)

		            if err != nil {

		                return fmt.Errorf("failed to create output file %s: %w", outputFileName, err)

		            }

		            defer file.Close()

		

		            for _, name := range currentBatch {

		                _, err := file.WriteString(name + "\n")

		                if err != nil {

		                    return fmt.Errorf("failed to write to file %s: %w", outputFileName, err)

		                }

		            }

		            log.Printf("Wrote %d leaf names to %s", len(currentBatch), outputFileName)

		            currentBatch = nil // Reset batch

		            batchNum++

		            return nil

		        }

		

		        for leafName := range leafNamesChan {

		            currentBatch = append(currentBatch, leafName)

		            if len(currentBatch) >= maxCount {

		                if err := writeBatchToFile(); err != nil {

		                    log.Printf("ERROR: %v", err)

		                }

		            }

		        }

		        // Write any remaining items in the last batch

		        if err := writeBatchToFile(); err != nil {

		            log.Printf("ERROR: %v", err)

		        }

		    }(outputLeafName)

	// 3. Discover partitions and send them to partitionsChan
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(rootPrefix),
		Delimiter: aws.String("/"), // Discover common prefixes (partitions)
	})

	log.Printf("Discovering partitions under s3://%s/%s...", bucket, rootPrefix)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to list common prefixes: %w", err)
			}

		// Enqueue common prefixes (partitions) for scanning
		for _, commonPrefix := range page.CommonPrefixes {
			if commonPrefix.Prefix != nil {
				partitionsChan <- *commonPrefix.Prefix
			}
		}
		// Also enqueue the rootPrefix itself if it contains objects directly
		if len(page.Contents) > 0 && rootPrefix != "" {
			partitionsChan <- rootPrefix
		}
	}

	close(partitionsChan) // No more partitions to send

	// Wait for all scanner goroutines to finish their work
	wgScanners.Wait()
	close(leafNamesChan) // All leaf names have been sent

	// Wait for the writer goroutine to finish processing all leaf names
	wgWriter.Wait()

	log.Printf("Batching complete.")

	return nil
}
