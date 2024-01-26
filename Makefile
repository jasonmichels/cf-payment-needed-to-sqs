.PHONY: build clean

build:
	@CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
	@zip getpaymentneeded.zip main

test:
	go test -v ./... -bench . -cover

clean:
	@rm -f main getpaymentneeded.zip
