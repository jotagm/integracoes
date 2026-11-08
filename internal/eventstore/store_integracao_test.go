//go:build integracao

// Testes contra um Postgres de verdade, num container descartável.
// Rodam com: go test -race -tags integracao ./...  (precisa do Docker)
package eventstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// Um container para o pacote inteiro. Cada teste usa um nome de fonte próprio,
// então eles não se enxergam sem precisar apagar dados (o evento é append-only).
var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("lab"),
		postgres.WithUsername("lab"),
		postgres.WithPassword("lab"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("subir postgres: %v", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("dsn: %v", err)
	}
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	if err := Migrar(ctx, pool); err != nil {
		log.Fatalf("migrar: %v", err)
	}

	code := m.Run()

	pool.Close()
	_ = ctr.Terminate(ctx)
	os.Exit(code)
}

func ativ(f atividade.Fonte, id string) atividade.Atividade {
	t := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	return atividade.Atividade{
		VersaoSchema: atividade.VersaoSchema, Fonte: f, IDExterno: id,
		Tipo: atividade.CommitEnviado, Quando: t, ColetadoEm: t,
		Dados: []byte(`{"repositorio":"jotagm/integracoes"}`),
	}
}

func lote(f atividade.Fonte, ids ...string) []atividade.Atividade {
	as := make([]atividade.Atividade, 0, len(ids))
	for _, id := range ids {
		as = append(as, ativ(f, id))
	}
	return as
}

// contar devolve quantos eventos e quantas linhas de outbox a fonte tem.
func contar(t *testing.T, f atividade.Fonte) (eventos, outbox int) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM evento WHERE fonte = $1`, string(f)).Scan(&eventos); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox o JOIN evento e ON e.seq = o.evento_seq WHERE e.fonte = $1`, string(f),
	).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	return eventos, outbox
}

func TestGravarEDedup(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-dedup")

	novas, err := s.Gravar(ctx, f, lote(f, "1", "2", "3"), fonte.Cursor{ETag: "v1"})
	if err != nil || novas != 3 {
		t.Fatalf("primeira gravação: novas=%d err=%v", novas, err)
	}

	// Mesma janela de novo (o polling se sobrepõe): nada novo, mas o cursor avança.
	novas, err = s.Gravar(ctx, f, lote(f, "1", "2", "3"), fonte.Cursor{ETag: "v2"})
	if err != nil || novas != 0 {
		t.Fatalf("regravação: novas=%d err=%v", novas, err)
	}

	eventos, outbox := contar(t, f)
	if eventos != 3 || outbox != 3 {
		t.Fatalf("esperava 3 eventos e 3 na outbox, veio %d e %d", eventos, outbox)
	}
	c, err := s.Cursor(ctx, f)
	if err != nil || c.ETag != "v2" {
		t.Fatalf("cursor: %+v err=%v", c, err)
	}
}

func TestLoteParcialmenteRepetido(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-parcial")

	if _, err := s.Gravar(ctx, f, lote(f, "a", "b"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}
	novas, err := s.Gravar(ctx, f, lote(f, "a", "b", "c"), fonte.Cursor{})
	if err != nil || novas != 1 {
		t.Fatalf("só o c era novo: novas=%d err=%v", novas, err)
	}
}

func TestMesmoIDDuasVezesNoLote(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-repetido-no-lote")

	novas, err := s.Gravar(ctx, f, lote(f, "x", "x"), fonte.Cursor{})
	if err != nil || novas != 1 {
		t.Fatalf("o segundo x cai no UNIQUE: novas=%d err=%v", novas, err)
	}
}

func TestCursorDeFonteNuncaVista(t *testing.T) {
	c, err := Novo(pool).Cursor(context.Background(), "fonte-que-nao-existe")
	if err != nil || c != (fonte.Cursor{}) {
		t.Fatalf("esperava cursor vazio, veio %+v err=%v", c, err)
	}
}

func TestLoteInvalidoNaoGravaNada(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-invalido")

	misturado := append(lote(f, "1"), ativ("outra-fonte", "2"))
	if _, err := s.Gravar(ctx, f, misturado, fonte.Cursor{ETag: "nao-deveria-gravar"}); err == nil {
		t.Fatal("lote com fonte misturada deveria falhar")
	}

	eventos, _ := contar(t, f)
	c, _ := s.Cursor(ctx, f)
	if eventos != 0 || c.ETag != "" {
		t.Fatalf("nada deveria ter sido gravado: eventos=%d cursor=%+v", eventos, c)
	}
}

func TestEventoEAppendOnly(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-append-only")
	if _, err := s.Gravar(ctx, f, lote(f, "1"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	for _, sql := range []string{
		`UPDATE evento SET tipo = 'Outro' WHERE fonte = 'teste-append-only'`,
		`DELETE FROM evento WHERE fonte = 'teste-append-only'`,
		`TRUNCATE evento CASCADE`,
	} {
		_, err := pool.Exec(ctx, sql)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%q deveria ser bloqueado pelo trigger, veio %v", sql, err)
		}
	}
}

func TestGravacoesConcorrentesNaoDuplicam(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-concorrencia")
	mesmoLote := lote(f, "1", "2", "3", "4", "5")

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	for g := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			novas, err := s.Gravar(ctx, f, mesmoLote, fonte.Cursor{Valor: fmt.Sprint(g)})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total += novas
			mu.Unlock()
		}()
	}
	wg.Wait()

	eventos, outbox := contar(t, f)
	if total != 5 || eventos != 5 || outbox != 5 {
		t.Fatalf("10 gravações simultâneas do mesmo lote: novas=%d eventos=%d outbox=%d (esperava 5)", total, eventos, outbox)
	}
}

func TestSeqCresceNaOrdemDeGravacao(t *testing.T) {
	ctx := context.Background()
	s := Novo(pool)
	f := atividade.Fonte("teste-seq")
	if _, err := s.Gravar(ctx, f, lote(f, "1", "2", "3"), fonte.Cursor{}); err != nil {
		t.Fatal(err)
	}

	rows, err := pool.Query(ctx, `SELECT id_externo FROM evento WHERE fonte = $1 ORDER BY seq`, string(f))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ordem []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ordem = append(ordem, id)
	}
	if strings.Join(ordem, ",") != "1,2,3" {
		t.Fatalf("ordem por seq: %v", ordem)
	}
}

func TestMigrarDeNovoNaoFazNada(t *testing.T) {
	if err := Migrar(context.Background(), pool); err != nil {
		t.Fatalf("rodar as migrações de novo deveria ser seguro: %v", err)
	}
}
