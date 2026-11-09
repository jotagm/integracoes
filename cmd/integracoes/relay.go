package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jotagm/integracoes/internal/eventstore"
	"github.com/jotagm/integracoes/internal/kafka"
	"github.com/jotagm/integracoes/internal/relay"
)

const (
	topicoPadrao = "atividades"
	// Mais partições que fontes: deixa espaço para outras fontes sem recriar o tópico.
	particoesPadrao = 3
)

// rodarRelay lê a outbox e publica no Kafka até o SIGTERM.
//
//	DATABASE_URL   obrigatório (o mesmo do ingestor)
//	KAFKA_BROKERS  obrigatório, separado por vírgula (ex.: localhost:9092)
//	KAFKA_TOPICO   opcional, padrão "atividades"
func rodarRelay(ctx context.Context, _ []string) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("variável DATABASE_URL não definida")
	}
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		return errors.New("variável KAFKA_BROKERS não definida")
	}
	topico := os.Getenv("KAFKA_TOPICO")
	if topico == "" {
		topico = topicoPadrao
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("abrir pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("conectar no postgres: %w", err)
	}
	if err := eventstore.Migrar(ctx, pool); err != nil {
		return err
	}

	if err := kafka.GarantirTopico(ctx, brokers, topico, particoesPadrao); err != nil {
		return err
	}
	produtor, err := kafka.NovoProdutor(brokers, topico)
	if err != nil {
		return err
	}
	defer produtor.Fechar()

	slog.Info("relay configurado", "topico", topico, "brokers", brokers)
	relay.Novo(eventstore.Novo(pool), produtor, slog.Default()).Rodar(ctx)
	return nil
}
