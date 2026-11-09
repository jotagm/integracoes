// Package kafka é o adaptador do Kafka (franz-go): o produtor que o relay usa
// e a criação do tópico.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jotagm/integracoes/internal/relay"
)

// Produtor implementa relay.Publicador.
//
// O franz-go já liga o produtor idempotente por padrão, com acks de todas as
// réplicas em sincronia. Com isso, uma retentativa interna do cliente não duplica
// mensagem no broker e a ordem dentro de cada partição é preservada.
type Produtor struct {
	cl *kgo.Client
}

// NovoProdutor conecta nos brokers e publica sempre no tópico informado.
func NovoProdutor(brokers []string, topico string) (*Produtor, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topico),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: criar produtor: %w", err)
	}
	return &Produtor{cl: cl}, nil
}

// Publicar manda o lote e espera a confirmação de todas as mensagens.
// A chave (a fonte) decide a partição: mesma fonte, mesma partição, mesma ordem.
func (p *Produtor) Publicar(ctx context.Context, msgs []relay.Mensagem) error {
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = &kgo.Record{Key: m.Chave, Value: m.Valor}
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publicar lote de %d: %w", len(msgs), err)
	}
	return nil
}

// Fechar espera as mensagens em voo e fecha as conexões.
func (p *Produtor) Fechar() {
	p.cl.Close()
}

// GarantirTopico cria o tópico se ele não existir. Rodar de novo não faz nada.
// Replicação 1 porque o lab tem um broker só.
func GarantirTopico(ctx context.Context, brokers []string, topico string, particoes int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka: conectar para criar tópico: %w", err)
	}
	defer cl.Close()

	resp, err := kadm.NewClient(cl).CreateTopics(ctx, particoes, 1, nil, topico)
	if err != nil {
		return fmt.Errorf("kafka: criar tópico %s: %w", topico, err)
	}
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: criar tópico %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}
