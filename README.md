# integracoes

Pipeline CQRS com event sourcing em Go. Ingestores do Spotify e do GitHub rodam num lab
privado, gravam num event store (Postgres) e publicam num Kafka. Um projetor monta os
modelos de leitura e os empurra por gRPC para uma API pública, só de leitura.

> Em construção. Plano e cronograma no documento do projeto.

## Estrutura

```
cmd/integracoes/          binário único com subcomandos (ingestor, relay, projetor, leitura, replay)
internal/atividade/       evento normalizado e validação (sem IO)
internal/fonte/           contrato das fontes (porta) e classes de erro
internal/fontes/github/   adaptador do GitHub: cliente HTTP, normalização, Fonte
internal/fontes/spotify/  adaptador do Spotify: tokens, cliente, normalização com lacuna, Fonte
internal/ingestor/        laço por fonte: intervalo, limite, backoff, parada em erro permanente
internal/eventstore/      Postgres: evento (append-only), cursor_fonte, outbox, migrações goose
internal/espera/          backoff exponencial com jitter e espera que respeita cancelamento
internal/mensagem/        formato do evento no Kafka (contrato relay -> projetor)
internal/relay/           laço outbox -> Kafka, at-least-once
internal/kafka/           adaptador franz-go: produtor e criação de tópico
deploy/lab/               docker compose do lado de escrita
proto/                    contrato gRPC entre o lab e o lado público (buf)
```

## Rodando

```bash
cp .env.example .env   # e preencha
make lab-up            # sobe o Postgres e o Kafka do lab
make ingestor          # roda o ingestor com o .env
go run ./cmd/integracoes relay   # outbox -> Kafka (com o .env carregado)

make test     # go test -race ./...
make test-integracao   # com Postgres de verdade (precisa do Docker)
make cover    # cobertura por função
make build    # bin/integracoes
make proto    # buf lint + buf generate (precisa do buf)
```

## Status

- [x] Dia 1: esqueleto, `Atividade`, normalização do GitHub com testes, contrato gRPC, CI
- [x] Dia 2: cliente do GitHub (ETag/304, `X-Poll-Interval`, 429/403 de limite, erros transitórios e permanentes)
- [x] Dia 3: fonte do Spotify (refresh token com cache e rotação, cursor `after`, retry em 401, lacuna)
- [x] Dia 4: event store no Postgres (append-only, dedup, outbox, cursor numa transação) e ingestor com backoff
- [x] Dia 5: relay da outbox para o Kafka (lote travado com FOR UPDATE, at-least-once, ordem por fonte)
- [ ] Dia 6: projetor
