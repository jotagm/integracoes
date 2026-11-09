package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/mensagem"
)

// --- dublês -----------------------------------------------------------------

// outboxMemoria imita a semântica do Postgres: só marca se publicar devolver nil.
type outboxMemoria struct {
	mu        sync.Mutex
	pendentes []mensagem.Evento
	marcados  []int64
	falhar    error // erro do próprio banco, uma vez
}

func (o *outboxMemoria) ProcessarLote(ctx context.Context, limite int, publicar func(context.Context, []mensagem.Evento) error) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.falhar != nil {
		err := o.falhar
		o.falhar = nil
		return 0, err
	}
	n := min(limite, len(o.pendentes))
	lote := o.pendentes[:n]
	if n == 0 {
		return 0, nil
	}
	if err := publicar(ctx, lote); err != nil {
		return 0, err // rollback: nada sai dos pendentes
	}
	for _, e := range lote {
		o.marcados = append(o.marcados, e.Seq)
	}
	o.pendentes = o.pendentes[n:]
	return n, nil
}

// publicadorMemoria guarda o que recebeu e pode falhar nas primeiras N chamadas.
type publicadorMemoria struct {
	mu          sync.Mutex
	recebidas   []Mensagem
	falharVezes int
	chamadas    int
}

func (p *publicadorMemoria) Publicar(_ context.Context, msgs []Mensagem) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chamadas++
	if p.falharVezes > 0 {
		p.falharVezes--
		return errors.New("broker indisponível")
	}
	p.recebidas = append(p.recebidas, msgs...)
	return nil
}

func evento(seq int64, fonte string) mensagem.Evento {
	t := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	return mensagem.Evento{
		Seq: seq, Fonte: fonte, IDExterno: fmt.Sprint(seq), Tipo: "CommitEnviado",
		VersaoSchema: 1, Quando: t, ColetadoEm: t, Dados: json.RawMessage(`{}`),
	}
}

func eventos(n int) []mensagem.Evento {
	es := make([]mensagem.Evento, 0, n)
	for i := 1; i <= n; i++ {
		fonte := "github"
		if i%2 == 0 {
			fonte = "spotify"
		}
		es = append(es, evento(int64(i), fonte))
	}
	return es
}

// rodarAte roda o relay até `pare` ficar verdadeiro (checado a cada espera) e
// devolve as esperas pedidas. Nenhuma espera dorme de verdade.
func rodarAte(t *testing.T, r *Relay, pare func() bool) []time.Duration {
	t.Helper()
	ctx, cancelar := context.WithCancel(context.Background())
	defer cancelar()
	var esperas []time.Duration
	r.esperar = func(ctx context.Context, d time.Duration) error {
		esperas = append(esperas, d)
		if pare() || len(esperas) > 50 {
			cancelar()
		}
		return ctx.Err()
	}
	r.Rodar(ctx)
	return esperas
}

func novoRelay(o Outbox, p Publicador) *Relay {
	r := Novo(o, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.backoff.Jitter = func(d time.Duration) time.Duration { return d }
	return r
}

// --- testes -----------------------------------------------------------------

func TestPublicaEmOrdemEMarcaTudo(t *testing.T) {
	o := &outboxMemoria{pendentes: eventos(5)}
	p := &publicadorMemoria{}
	r := novoRelay(o, p)

	rodarAte(t, r, func() bool { return len(o.pendentes) == 0 })

	if len(p.recebidas) != 5 || len(o.marcados) != 5 {
		t.Fatalf("publicadas=%d marcadas=%d", len(p.recebidas), len(o.marcados))
	}
	for i, m := range p.recebidas {
		e, err := mensagem.Decodificar(m.Valor)
		if err != nil {
			t.Fatal(err)
		}
		if e.Seq != int64(i+1) {
			t.Fatalf("posição %d: esperava seq %d, veio %d", i, i+1, e.Seq)
		}
		if string(m.Chave) != e.Fonte {
			t.Fatalf("chave %q diferente da fonte %q", m.Chave, e.Fonte)
		}
	}
}

func TestLoteCheioNaoEspera(t *testing.T) {
	o := &outboxMemoria{pendentes: eventos(250)}
	p := &publicadorMemoria{}
	r := novoRelay(o, p)

	esperas := rodarAte(t, r, func() bool { return len(o.pendentes) == 0 })

	// 100 + 100 seguem direto; o lote de 50 não é cheio e só então o relay espera.
	if p.chamadas != 3 || len(esperas) != 1 || esperas[0] != time.Second {
		t.Fatalf("chamadas=%d esperas=%v", p.chamadas, esperas)
	}
}

func TestFalhaAoPublicarNaoMarcaEFazBackoff(t *testing.T) {
	o := &outboxMemoria{pendentes: eventos(3)}
	p := &publicadorMemoria{falharVezes: 2}
	r := novoRelay(o, p)

	esperas := rodarAte(t, r, func() bool { return len(o.pendentes) == 0 })

	if len(o.marcados) != 3 || len(p.recebidas) != 3 {
		t.Fatalf("depois de voltar, tudo publicado uma vez: marcados=%d recebidas=%d", len(o.marcados), len(p.recebidas))
	}
	if fmt.Sprint(esperas[:2]) != fmt.Sprint([]time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("backoff: %v", esperas)
	}
}

func TestErroDoBancoFazBackoff(t *testing.T) {
	o := &outboxMemoria{pendentes: eventos(1), falhar: errors.New("conexão caiu")}
	p := &publicadorMemoria{}
	r := novoRelay(o, p)

	esperas := rodarAte(t, r, func() bool { return len(o.pendentes) == 0 })

	if esperas[0] != time.Second || len(o.marcados) != 1 {
		t.Fatalf("esperas=%v marcados=%d", esperas, len(o.marcados))
	}
}

func TestEventoInvalidoNaoPublicaNada(t *testing.T) {
	quebrado := evento(2, "github")
	quebrado.Dados = nil
	o := &outboxMemoria{pendentes: []mensagem.Evento{evento(1, "github"), quebrado}}
	p := &publicadorMemoria{}
	r := novoRelay(o, p)

	rodarAte(t, r, func() bool { return true })

	// O lote inteiro para: publicar o seq 1 sem o 2 quebraria a ordem.
	if p.chamadas != 0 || len(o.marcados) != 0 {
		t.Fatalf("nada deveria sair: chamadas=%d marcados=%d", p.chamadas, len(o.marcados))
	}
}

func TestOutboxVaziaEsperaOIntervalo(t *testing.T) {
	r := novoRelay(&outboxMemoria{}, &publicadorMemoria{})
	esperas := rodarAte(t, r, func() bool { return true })
	if len(esperas) != 1 || esperas[0] != time.Second {
		t.Fatalf("esperas: %v", esperas)
	}
}
