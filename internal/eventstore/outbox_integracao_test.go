//go:build integracao

package eventstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
	"github.com/jotagm/integracoes/internal/mensagem"
)

// Os testes de outbox enxergam pendentes de todas as fontes (a outbox é global),
// então cada um primeiro esvazia o que os outros testes deixaram pendente.
func esvaziarOutbox(t *testing.T, s *Store) {
	t.Helper()
	for {
		n, err := s.ProcessarLote(context.Background(), 1000, func(context.Context, []mensagem.Evento) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func TestOutboxPublicaEmOrdemEMarca(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	esvaziarOutbox(t, s)
	f := atividade.Fonte("teste-outbox-ordem")
	if _, err := s.Gravar(ctx, f, lote(f, "1", "2", "3"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	var recebidos []mensagem.Evento
	n, err := s.ProcessarLote(ctx, 10, func(_ context.Context, es []mensagem.Evento) error {
		recebidos = append(recebidos, es...)
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for i := 1; i < len(recebidos); i++ {
		if recebidos[i].Seq <= recebidos[i-1].Seq {
			t.Fatalf("fora de ordem: %d depois de %d", recebidos[i].Seq, recebidos[i-1].Seq)
		}
	}
	if string(recebidos[0].Dados) == "" || recebidos[0].IDExterno != "1" || recebidos[0].Quando.Location() != time.UTC {
		t.Fatalf("evento lido errado: %+v", recebidos[0])
	}

	// Já marcados: o próximo lote vem vazio.
	n, err = s.ProcessarLote(ctx, 10, func(context.Context, []mensagem.Evento) error { return nil })
	if err != nil || n != 0 {
		t.Fatalf("deveria estar vazio: n=%d err=%v", n, err)
	}
}

func TestOutboxFalhaAoPublicarNaoMarca(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	esvaziarOutbox(t, s)
	f := atividade.Fonte("teste-outbox-falha")
	if _, err := s.Gravar(ctx, f, lote(f, "1", "2"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	falha := errors.New("broker caiu")
	if _, err := s.ProcessarLote(ctx, 10, func(context.Context, []mensagem.Evento) error { return falha }); !errors.Is(err, falha) {
		t.Fatalf("esperava o erro do publicar, veio %v", err)
	}

	// Continuam pendentes e saem no próximo lote.
	n, err := s.ProcessarLote(ctx, 10, func(context.Context, []mensagem.Evento) error { return nil })
	if err != nil || n != 2 {
		t.Fatalf("os 2 deveriam continuar pendentes: n=%d err=%v", n, err)
	}
}

func TestOutboxRespeitaOLimite(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	esvaziarOutbox(t, s)
	f := atividade.Fonte("teste-outbox-limite")
	if _, err := s.Gravar(ctx, f, lote(f, "1", "2", "3", "4", "5"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	tamanhos := []int{}
	for {
		n, err := s.ProcessarLote(ctx, 2, func(context.Context, []mensagem.Evento) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		tamanhos = append(tamanhos, n)
	}
	if len(tamanhos) != 3 || tamanhos[0] != 2 || tamanhos[2] != 1 {
		t.Fatalf("lotes de 2: %v", tamanhos)
	}
}

func TestDoisRelaysNaoPublicamOMesmoEvento(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	esvaziarOutbox(t, s)
	f := atividade.Fonte("teste-outbox-concorrencia")
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	if _, err := s.Gravar(ctx, f, lote(f, ids...), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	var (
		mu     sync.Mutex
		vistos = map[int64]int{}
		wg     sync.WaitGroup
	)
	publicar := func(_ context.Context, es []mensagem.Evento) error {
		time.Sleep(5 * time.Millisecond) // segura o lock para o outro relay esbarrar nele
		mu.Lock()
		defer mu.Unlock()
		for _, e := range es {
			vistos[e.Seq]++
		}
		return nil
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := s.ProcessarLote(ctx, 7, publicar)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(vistos) != 50 {
		t.Fatalf("esperava 50 eventos publicados, veio %d", len(vistos))
	}
	for seq, n := range vistos {
		if n != 1 {
			t.Fatalf("seq %d publicado %d vezes por relays concorrentes", seq, n)
		}
	}
}
