
name = ""
version = "0.1.0"

.PHONY: build_docker
.PHONY: build
.PHONY: test

TEST_DIRS=$(go list ./... | grep -v /app/services/api)

test:
	go test .

build:
	go build 

clean:
	rm -rf build