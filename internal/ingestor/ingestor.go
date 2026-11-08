// Package ingestor roda as fontes em laço e grava o que elas coletam no event store.
//
// Cada fonte roda na sua própria goroutine, então uma fonte com problema não
// atrasa a outra. A decisão de cada ciclo depende só da classe do erro:
//
//	sucesso          -> espera o intervalo que a fonte pediu
//	fonte.ErrLimite  -> espera o tempo que a API mandou
//	fonte.ErrPermanente -> para aquela fonte (precisa de gente: credencial, config)
//	qualquer outro   -> backoff exponencial com jitter (inclui erro do banco)
package ingestor

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// Repositorio é o que o ingestor precisa do event store. A interface fica aqui,
// do lado de quem usa, para o teste do laço não depender de Postgres.
type Repositorio interface {
	Cursor(ctx context.Context, f atividade.Fonte) (fonte.Cursor, error)
	// Gravar grava atividades, cursor e outbox numa transação só e devolve quantas eram novas.
	Gravar(ctx context.Context, f atividade.Fonte, atividades []atividade.Atividade, cursor fonte.Cursor) (int, error)
}

// Ingestor coordena as fontes.
type Ingestor struct {
	repo       Repositorio
	log        *slog.Logger
	backoffMin time.Duration
	backoffMax time.Duration

	// Injetáveis para o teste não dormir de verdade nem depender de sorte.
	esperar func(ctx context.Context, d time.Duration) error
	jitter  func(d time.Duration) time.Duration
}

// Novo cria o ingestor com backoff de 1 s a 5 min.
func Novo(repo Repositorio, log *slog.Logger) *Ingestor {
	return &Ingestor{
		repo:       repo,
		log:        log,
		backoffMin: time.Second,
		backoffMax: 5 * time.Minute,
		esperar:    esperar,
		jitter:     meioJitter,
	}
}

// Rodar roda todas as fontes até o contexto ser cancelado ou todas pararem.
func (i *Ingestor) Rodar(ctx context.Context, fontes ...fonte.Fonte) {
	var wg sync.WaitGroup
	for _, f := range fontes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.rodarFonte(ctx, f)
		}()
	}
	wg.Wait()
}

func (i *Ingestor) rodarFonte(ctx context.Context, f fonte.Fonte) {
	log := i.log.With("fonte", f.Nome())
	log.Info("fonte iniciada")
	falhas := 0

	for {
		espera, err := i.ciclo(ctx, f, log)

		var limite *fonte.ErrLimite
		switch {
		case err == nil:
			falhas = 0
		case ctx.Err() != nil:
			log.Info("fonte encerrada")
			return
		case errors.As(err, &limite):
			espera = limite.RetryAfter
			log.Warn("limite da API, aguardando", "espera", espera.String())
		case errors.Is(err, fonte.ErrPermanente):
			log.Error("fonte parada: erro permanente, precisa de intervenção", "erro", err)
			return
		default:
			falhas++
			espera = i.backoff(falhas)
			log.Warn("falha transitória, tentando de novo", "erro", err, "tentativa", falhas, "espera", espera.String())
		}

		if err := i.esperar(ctx, espera); err != nil {
			log.Info("fonte encerrada")
			return
		}
	}
}

// ciclo faz um poll e grava. Devolve quanto esperar até o próximo.
func (i *Ingestor) ciclo(ctx context.Context, f fonte.Fonte, log *slog.Logger) (time.Duration, error) {
	cursor, err := i.repo.Cursor(ctx, f.Nome())
	if err != nil {
		return 0, err
	}

	coleta, err := f.Coletar(ctx, cursor)
	if err != nil {
		return 0, err
	}

	// Nada novo e cursor igual: não há o que gravar (o caso comum com o 304 do GitHub).
	if len(coleta.Atividades) == 0 && coleta.NovoCursor == cursor {
		return coleta.ProximoPoll, nil
	}

	novas, err := i.repo.Gravar(ctx, f.Nome(), coleta.Atividades, coleta.NovoCursor)
	if err != nil {
		// A transação voltou atrás, cursor incluído: o próximo poll pega tudo de novo.
		return 0, err
	}
	log.Info("ciclo gravado",
		"recebidas", len(coleta.Atividades),
		"novas", novas,
		"repetidas", len(coleta.Atividades)-novas,
		"descartes", coleta.Descartes,
		"proximo_poll", coleta.ProximoPoll.String())
	return coleta.ProximoPoll, nil
}

// backoff dobra a espera a cada falha seguida, até backoffMax, com jitter.
func (i *Ingestor) backoff(falhas int) time.Duration {
	d := i.backoffMin
	for n := 1; n < falhas && d < i.backoffMax; n++ {
		d *= 2
	}
	return i.jitter(min(d, i.backoffMax))
}

// meioJitter devolve um valor entre d/2 e d: espalha as tentativas sem nunca esperar quase zero.
func meioJitter(d time.Duration) time.Duration {
	metade := d / 2
	return metade + rand.N(metade+1)
}

func esperar(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
