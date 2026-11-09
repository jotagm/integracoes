// Package relay leva os eventos da outbox para o Kafka.
//
// Garantia: at-least-once. Cada lote é travado na outbox, publicado e só então
// marcado, numa transação. Se o processo cair entre publicar e marcar, o lote
// é publicado de novo no próximo ciclo; o projetor deduplica pelo seq.
// O que nunca acontece é marcar sem ter publicado (perder evento).
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jotagm/integracoes/internal/espera"
	"github.com/jotagm/integracoes/internal/mensagem"
)

// Outbox é o que o relay precisa do event store.
type Outbox interface {
	// ProcessarLote trava até `limite` eventos pendentes, em ordem de seq, chama
	// publicar com eles e, se publicar der certo, marca como publicados. Tudo numa
	// transação: se publicar falhar, nada é marcado. Devolve quantos processou.
	ProcessarLote(ctx context.Context, limite int, publicar func(context.Context, []mensagem.Evento) error) (int, error)
}

// Mensagem é o que vai para o Kafka.
type Mensagem struct {
	Chave []byte
	Valor []byte
}

// Publicador manda um lote para o Kafka e só volta sem erro quando todas as
// mensagens foram confirmadas pelo broker, na ordem em que foram passadas.
type Publicador interface {
	Publicar(ctx context.Context, msgs []Mensagem) error
}

// Relay roda o laço outbox -> Kafka.
type Relay struct {
	outbox      Outbox
	publicador  Publicador
	log         *slog.Logger
	tamanhoLote int
	// intervalo é a espera quando a outbox está vazia.
	intervalo time.Duration
	backoff   espera.Backoff
	esperar   func(ctx context.Context, d time.Duration) error
}

// Novo cria o relay com lotes de 100 e polling de 1 s quando não há pendentes.
func Novo(o Outbox, p Publicador, log *slog.Logger) *Relay {
	return &Relay{
		outbox:      o,
		publicador:  p,
		log:         log,
		tamanhoLote: 100,
		intervalo:   time.Second,
		backoff:     espera.Padrao(),
		esperar:     espera.Esperar,
	}
}

// Rodar processa lotes até o contexto ser cancelado.
func (r *Relay) Rodar(ctx context.Context) {
	r.log.Info("relay iniciado", "lote", r.tamanhoLote)
	falhas := 0

	for {
		n, err := r.outbox.ProcessarLote(ctx, r.tamanhoLote, r.publicar)

		var pausa time.Duration
		switch {
		case ctx.Err() != nil:
			r.log.Info("relay encerrado")
			return
		case err != nil:
			falhas++
			pausa = r.backoff.Duracao(falhas)
			r.log.Warn("lote falhou, nada foi marcado; tentando de novo", "erro", err, "tentativa", falhas, "espera", pausa.String())
		case n == r.tamanhoLote:
			// Lote cheio: provavelmente tem mais esperando. Segue sem dormir.
			falhas = 0
			r.log.Info("lote publicado", "eventos", n)
			continue
		case n > 0:
			falhas = 0
			r.log.Info("lote publicado", "eventos", n)
			pausa = r.intervalo
		default:
			falhas = 0
			pausa = r.intervalo
		}

		if err := r.esperar(ctx, pausa); err != nil {
			r.log.Info("relay encerrado")
			return
		}
	}
}

// publicar converte os eventos e manda o lote inteiro de uma vez.
func (r *Relay) publicar(ctx context.Context, eventos []mensagem.Evento) error {
	msgs := make([]Mensagem, 0, len(eventos))
	for _, e := range eventos {
		valor, err := e.Codificar()
		if err != nil {
			// Evento inválido no event store é bug, não falha de rede: o lote para e o log mostra.
			return fmt.Errorf("relay: evento seq %d: %w", e.Seq, err)
		}
		msgs = append(msgs, Mensagem{Chave: e.Chave(), Valor: valor})
	}
	return r.publicador.Publicar(ctx, msgs)
}
