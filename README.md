# integracoes

Pipeline CQRS com event sourcing em Go. Ingestores do Spotify e do GitHub rodam num lab
privado, gravam num event store (Postgres) e publicam num Kafka. Um projetor monta os
modelos de leitura e os empurra por gRPC para uma API pública, só de leitura.

> Em construção. Plano e cronograma no documento do projeto.

## Estrutura

```
cmd/integracoes/        binário único com subcomandos (ingestor, relay, projetor, leitura, replay)
internal/atividade/     evento normalizado e validação (sem IO)
internal/fontes/github/ normalização dos eventos públicos do GitHub
proto/                  contrato gRPC entre o lab e o lado público (buf)
```

## Rodando

```bash
make test     # go test -race ./...
make cover    # cobertura por função
make build    # bin/integracoes
make proto    # buf lint + buf generate (precisa do buf)
```

## Status

- [x] Dia 1: esqueleto, `Atividade`, normalização do GitHub com testes, contrato gRPC, CI
- [ ] Dia 2: cliente do GitHub (ETag/304, `X-Poll-Interval`, 429)
- [ ] Dia 3: cliente do Spotify (refresh token, cursor, lacuna)
