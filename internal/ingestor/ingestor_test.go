package ingestor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// --- dublês -----------------------------------------------------------------

// fonteRoteirizada devolve, a cada Coletar, o próximo passo do roteiro.
type fonteRoteirizada struct {
	nome     atividade.Fonte
	mu       sync.Mutex
	roteiro  []passo
	cursores []fonte.Cursor // cursores que o ingestor passou
}

type passo struct {
	coleta fonte.Coleta
	err    error
}

func (f *fonteRoteirizada) Nome() atividade.Fonte { return f.nome }

func (f *fonteRoteirizada) Coletar(_ context.Context, c fonte.Cursor) (fonte.Coleta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursores = append(f.cursores, c)
	if len(f.roteiro) == 0 {
		return fonte.Coleta{}, fmt.Errorf("%w: roteiro acabou", fonte.ErrPermanente)
	}
	p := f.roteiro[0]
	f.roteiro = f.roteiro[1:]
	return p.coleta, p.err
}

// repoMemoria guarda em memória e simula o UNIQUE (fonte, id_externo).
type repoMemoria struct {
	mu           sync.Mutex
	vistos       map[string]bool
	cursores     map[atividade.Fonte]fonte.Cursor
	gravacoes    int
	falharGravar error
}

func novoRepo() *repoMemoria {
	return &repoMemoria{vistos: map[string]bool{}, cursores: map[atividade.Fonte]fonte.Cursor{}}
}

func (r *repoMemoria) Cursor(_ context.Context, f atividade.Fonte) (fonte.Cursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cursores[f], nil
}

func (r *repoMemoria) Gravar(_ context.Context, f atividade.Fonte, as []atividade.Atividade, c fonte.Cursor) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.falharGravar != nil {
		err := r.falharGravar
		r.falharGravar = nil // falha uma vez só
		return 0, err
	}
	r.gravacoes++
	novas := 0
	for _, a := range as {
		k := string(a.Fonte) + "|" + a.IDExterno
		if !r.vistos[k] {
			r.vistos[k] = true
			novas++
		}
	}
	r.cursores[f] = c
	return novas, nil
}

// novoTeste monta um ingestor cujo esperar só registra as esperas e nunca dorme.
func novoTeste(repo Repositorio) (*Ingestor, *[]time.Duration) {
	var esperas []time.Duration
	var mu sync.Mutex
	i := Novo(repo, slog.New(slog.NewTextHandler(io.Discard, nil)))
	i.jitter = func(d time.Duration) time.Duration { return d } // sem sorte nos testes
	i.esperar = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		defer mu.Unlock()
		esperas = append(esperas, d)
		return ctx.Err()
	}
	return i, &esperas
}

func ativ(id string) atividade.Atividade {
	t := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	return atividade.Atividade{VersaoSchema: 1, Fonte: atividade.FonteGitHub, IDExterno: id, Tipo: atividade.CommitEnviado, Quando: t, ColetadoEm: t, Dados: []byte(`{}`)}
}

func permanente() passo { return passo{err: fmt.Errorf("%w: fim do teste", fonte.ErrPermanente)} }

// --- testes -----------------------------------------------------------------

func TestSucessoGravaEAvancaOCursor(t *testing.T) {
	repo := novoRepo()
	f := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{
		{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("1"), ativ("2")}, NovoCursor: fonte.Cursor{ETag: "v1"}, ProximoPoll: 60 * time.Second}},
		{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("2"), ativ("3")}, NovoCursor: fonte.Cursor{ETag: "v2"}, ProximoPoll: 30 * time.Second}},
		permanente(),
	}}
	i, esperas := novoTeste(repo)

	i.Rodar(context.Background(), f)

	if len(repo.vistos) != 3 {
		t.Fatalf("esperava 3 eventos únicos, veio %d", len(repo.vistos))
	}
	if repo.cursores[atividade.FonteGitHub].ETag != "v2" {
		t.Fatalf("cursor final: %+v", repo.cursores)
	}
	// O segundo poll tem que receber o cursor gravado pelo primeiro.
	if f.cursores[1].ETag != "v1" || f.cursores[2].ETag != "v2" {
		t.Fatalf("cursores passados à fonte: %+v", f.cursores)
	}
	if fmt.Sprint(*esperas) != fmt.Sprint([]time.Duration{60 * time.Second, 30 * time.Second}) {
		t.Fatalf("esperas: %v", *esperas)
	}
}

func TestSemNovidadeNaoGrava(t *testing.T) {
	repo := novoRepo()
	f := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{
		{coleta: fonte.Coleta{NovoCursor: fonte.Cursor{}, ProximoPoll: time.Minute}}, // igual ao cursor vazio
		permanente(),
	}}
	i, _ := novoTeste(repo)
	i.Rodar(context.Background(), f)
	if repo.gravacoes != 0 {
		t.Fatalf("304 não deveria tocar no banco, gravou %d vezes", repo.gravacoes)
	}
}

func TestLimiteEsperaOQueAAPIMandou(t *testing.T) {
	f := &fonteRoteirizada{nome: atividade.FonteSpotify, roteiro: []passo{
		{err: &fonte.ErrLimite{RetryAfter: 42 * time.Second}},
		permanente(),
	}}
	i, esperas := novoTeste(novoRepo())
	i.Rodar(context.Background(), f)
	if len(*esperas) != 1 || (*esperas)[0] != 42*time.Second {
		t.Fatalf("esperas: %v", *esperas)
	}
}

func TestBackoffDobraEZeraNoSucesso(t *testing.T) {
	transitorio := passo{err: fmt.Errorf("%w: 503", fonte.ErrTransitorio)}
	ok := passo{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("1")}, ProximoPoll: time.Minute}}
	f := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{
		transitorio, transitorio, transitorio, ok, transitorio, permanente(),
	}}
	i, esperas := novoTeste(novoRepo())
	i.Rodar(context.Background(), f)

	quer := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, time.Minute, time.Second}
	if fmt.Sprint(*esperas) != fmt.Sprint(quer) {
		t.Fatalf("esperava %v, veio %v", quer, *esperas)
	}
}

func TestBackoffTemTeto(t *testing.T) {
	i, _ := novoTeste(novoRepo())
	// 1s dobrando: a 9ª falha espera 2^8 s = 4m16s; a 10ª já passaria de 5 min e fica no teto.
	if d := i.backoff(9); d != 256*time.Second {
		t.Fatalf("9ª falha: esperava 4m16s, veio %s", d)
	}
	for _, falhas := range []int{10, 11, 50, 1000} {
		if d := i.backoff(falhas); d != i.backoffMax {
			t.Fatalf("com %d falhas deveria ficar no teto %s, veio %s", falhas, i.backoffMax, d)
		}
	}
}

func TestErroDoBancoETratadoComoTransitorio(t *testing.T) {
	repo := novoRepo()
	repo.falharGravar = errors.New("conexão com o postgres caiu")
	lote := passo{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("1")}, NovoCursor: fonte.Cursor{ETag: "v1"}, ProximoPoll: time.Minute}}
	f := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{lote, lote, permanente()}}
	i, esperas := novoTeste(repo)

	i.Rodar(context.Background(), f)

	if (*esperas)[0] != time.Second {
		t.Fatalf("falha do banco deveria cair no backoff: %v", *esperas)
	}
	// Na segunda tentativa o cursor ainda é o vazio: a transação que falhou não avançou nada.
	if f.cursores[1].ETag != "" || len(repo.vistos) != 1 {
		t.Fatalf("cursor da 2ª tentativa=%+v, vistos=%d", f.cursores[1], len(repo.vistos))
	}
}

func TestPermanenteParaSoAQuelaFonte(t *testing.T) {
	repo := novoRepo()
	quebrada := &fonteRoteirizada{nome: atividade.FonteSpotify, roteiro: []passo{
		{err: fmt.Errorf("%w: refresh token revogado", fonte.ErrPermanente)},
	}}
	saudavel := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{
		{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("1")}, ProximoPoll: time.Minute}},
		{coleta: fonte.Coleta{Atividades: []atividade.Atividade{ativ("2")}, ProximoPoll: time.Minute}},
		permanente(),
	}}
	i, _ := novoTeste(repo)
	i.Rodar(context.Background(), quebrada, saudavel)

	if len(quebrada.cursores) != 1 {
		t.Fatalf("a fonte quebrada deveria tentar 1 vez e parar, tentou %d", len(quebrada.cursores))
	}
	if len(repo.vistos) != 2 {
		t.Fatalf("a fonte saudável deveria seguir gravando, gravou %d", len(repo.vistos))
	}
}

func TestCancelarEncerraOLaco(t *testing.T) {
	ctx, cancelar := context.WithCancel(context.Background())
	sempre := passo{coleta: fonte.Coleta{ProximoPoll: time.Minute}}
	f := &fonteRoteirizada{nome: atividade.FonteGitHub, roteiro: []passo{sempre, sempre, sempre, sempre}}
	i, _ := novoTeste(novoRepo())
	i.esperar = func(ctx context.Context, _ time.Duration) error {
		cancelar() // o "Ctrl+C" chega durante a primeira espera
		return ctx.Err()
	}

	terminou := make(chan struct{})
	go func() { i.Rodar(ctx, f); close(terminou) }()

	select {
	case <-terminou:
	case <-time.After(2 * time.Second):
		t.Fatal("o laço não encerrou depois do cancelamento")
	}
}

func TestMeioJitterFicaEntreMetadeEOTotal(t *testing.T) {
	for range 1000 {
		d := meioJitter(10 * time.Second)
		if d < 5*time.Second || d > 10*time.Second {
			t.Fatalf("fora da faixa: %s", d)
		}
	}
}

func TestEsperarRespeitaOContexto(t *testing.T) {
	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()
	if err := esperar(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("esperar com contexto cancelado deveria voltar na hora, veio %v", err)
	}
	if err := esperar(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}
