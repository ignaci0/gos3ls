.PHONY: build clean

BINARY_NAME=gos3ls

build:
	CGO_ENABLED=0 go build -o $(BINARY_NAME) -ldflags "-s -w" ./cmd/gos3ls
	chmod +x $(BINARY_NAME)

clean:
	rm -f $(BINARY_NAME)
