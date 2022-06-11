fmt:
	go fmt ./...

vet:
	go vet ./...

test: fmt vet
	go test ./... -coverprofile cover.out

coverage: test
	go tool cover -html=cover.out