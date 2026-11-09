//go:build integracao

// Testes contra um broker de verdade (Redpanda, compatível com o protocolo do Kafka
// e mais rápido de subir no CI). Rodam com: go test -race -tags integracao ./...
package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jotagm/integracoes/internal/relay"
)

var brokers []string

func TestMain(m *testing.M) {
	ctx := context.Background()
	ctr, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.18")
	if err != nil {
		log.Fatalf("subir redpanda: %v", err)
	}
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("seed broker: %v", err)
	}
	brokers = []string{broker}

	code := m.Run()
	_ = ctr.Terminate(ctx)
	os.Exit(code)
}

// consumirTudo lê n mensagens do início do tópico, ou falha depois de 30 s.
func consumirTudo(t *testing.T, topico string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topico),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()
	var recs []*kgo.Record
	for len(recs) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("leu %d de %d mensagens antes do timeout", len(recs), n)
		}
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	return recs
}

func TestGarantirTopicoEIdempotente(t *testing.T) {
	ctx := context.Background()
	for range 2 {
		if err := GarantirTopico(ctx, brokers, "teste-topico", 3); err != nil {
			t.Fatalf("criar o mesmo tópico duas vezes deveria ser seguro: %v", err)
		}
	}
}

func TestPublicaComOrdemPorChave(t *testing.T) {
	ctx := context.Background()
	const topico = "teste-ordem"
	if err := GarantirTopico(ctx, brokers, topico, 3); err != nil {
		t.Fatal(err)
	}
	p, err := NovoProdutor(brokers, topico)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Fechar()

	// 20 mensagens alternando duas chaves, publicadas em dois lotes.
	var lote1, lote2 []relay.Mensagem
	for i := 1; i <= 20; i++ {
		chave := "github"
		if i%2 == 0 {
			chave = "spotify"
		}
		m := relay.Mensagem{Chave: []byte(chave), Valor: []byte(fmt.Sprint(i))}
		if i <= 10 {
			lote1 = append(lote1, m)
		} else {
			lote2 = append(lote2, m)
		}
	}
	for _, l := range [][]relay.Mensagem{lote1, lote2} {
		if err := p.Publicar(ctx, l); err != nil {
			t.Fatal(err)
		}
	}

	recs := consumirTudo(t, topico, 20)

	// Entre partições não há ordem; dentro de cada chave, tem que haver.
	ultimo := map[string]int{}
	particao := map[string]int32{}
	for _, r := range recs {
		n, err := strconv.Atoi(string(r.Value))
		if err != nil {
			t.Fatal(err)
		}
		k := string(r.Key)
		if n <= ultimo[k] {
			t.Fatalf("chave %s: %d chegou depois de %d", k, n, ultimo[k])
		}
		ultimo[k] = n
		if anterior, ok := particao[k]; ok && anterior != r.Partition {
			t.Fatalf("chave %s apareceu em duas partições (%d e %d)", k, anterior, r.Partition)
		}
		particao[k] = r.Partition
	}
}
