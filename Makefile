.PHONY: test cover vet lint proto build

test:
	go test -race ./...

cover:
	go test -coverprofile=cobertura.out ./... && go tool cover -func=cobertura.out

vet:
	go vet ./...

lint:
	golangci-lint run ./...

# Gera o código Go do contrato gRPC em gen/. Precisa do buf instalado.
proto:
	buf lint && buf generate

build:
	go build -o bin/integracoes ./cmd/integracoes
