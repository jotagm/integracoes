.PHONY: test test-integracao cover vet lint proto build lab-up lab-down ingestor

test:
	go test -race ./...

# Testes contra Postgres de verdade (testcontainers). Precisa do Docker rodando.
test-integracao:
	go test -race -tags integracao ./...

cover:
	go test -coverprofile=cobertura.out ./... && go tool cover -func=cobertura.out

vet:
	go vet ./... && go vet -tags integracao ./...

lint:
	golangci-lint run ./...

# Gera o código Go do contrato gRPC em gen/. Precisa do buf instalado.
proto:
	buf lint && buf generate

build:
	go build -o bin/integracoes ./cmd/integracoes

lab-up:
	docker compose -f deploy/lab/docker-compose.yml --env-file .env up -d

lab-down:
	docker compose -f deploy/lab/docker-compose.yml --env-file .env down

# Roda o ingestor local, lendo o .env.
ingestor: build
	set -a && . ./.env && set +a && ./bin/integracoes ingestor