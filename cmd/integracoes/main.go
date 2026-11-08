// Comando único com subcomandos. Cada subcomando roda como um container separado
// no Docker Compose, mas o deploy continua sendo um binário só.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

var errNaoImplementado = errors.New("ainda não implementado")

var subcomandos = map[string]struct {
	descricao string
	rodar     func(ctx context.Context, args []string) error
}{
	"ingestor": {"coleta as fontes e grava no event store (dias 2 a 4)", naoImplementado},
	"relay":    {"publica a outbox no Kafka (dia 5)", naoImplementado},
	"projetor": {"consome o Kafka e empurra para a leitura (dias 6 e 7)", naoImplementado},
	"leitura":  {"servidor gRPC + API HTTP pública (dias 7 e 8)", naoImplementado},
	"replay":   {"reconstrói os modelos de leitura a partir do event store (dia 9)", naoImplementado},
}

func naoImplementado(context.Context, []string) error { return errNaoImplementado }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if len(os.Args) < 2 {
		uso()
		os.Exit(2)
	}

	nome := os.Args[1]
	sub, ok := subcomandos[nome]
	if !ok {
		fmt.Fprintf(os.Stderr, "subcomando desconhecido: %q\n\n", nome)
		uso()
		os.Exit(2)
	}

	// Cancela o contexto no Ctrl+C ou no SIGTERM do Docker: base do graceful shutdown.
	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	if err := sub.rodar(ctx, os.Args[2:]); err != nil {
		slog.Error("subcomando falhou", "subcomando", nome, "erro", err)
		os.Exit(1)
	}
}

func uso() {
	fmt.Fprintln(os.Stderr, "uso: integracoes <subcomando> [flags]")
	fmt.Fprintln(os.Stderr, "\nsubcomandos:")
	for _, nome := range []string{"ingestor", "relay", "projetor", "leitura", "replay"} {
		fmt.Fprintf(os.Stderr, "  %-9s %s\n", nome, subcomandos[nome].descricao)
	}
}
