.PHONY: build clean

build:
	@CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bootstrap .
	@zip getpaymentneeded.zip bootstrap

test:
	go test -v ./... -bench . -cover

clean:
	@rm -f bootstrap getpaymentneeded.zip
