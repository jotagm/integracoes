// Package eventstore grava as atividades no Postgres do lab: o event store
// append-only, o cursor de cada fonte e a outbox, sempre numa transação só.
package eventstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// Store implementa ingestor.Repositorio sobre o Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// Novo cria o Store. O pool é de quem chamou (quem abre, fecha).
func Novo(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Cursor devolve onde a fonte parou; uma fonte nunca vista devolve o cursor vazio.
func (s *Store) Cursor(ctx context.Context, f atividade.Fonte) (fonte.Cursor, error) {
	var c fonte.Cursor
	err := s.pool.QueryRow(ctx,
		`SELECT cursor, etag FROM cursor_fonte WHERE fonte = $1`, string(f),
	).Scan(&c.Valor, &c.ETag)
	if errors.Is(err, pgx.ErrNoRows) {
		return fonte.Cursor{}, nil
	}
	if err != nil {
		return fonte.Cursor{}, fmt.Errorf("eventstore: ler cursor de %s: %w", f, err)
	}
	return c, nil
}

// ON CONFLICT DO NOTHING + RETURNING: evento novo devolve o seq; repetido não devolve linha.
// É o dedup inteiro numa instrução, e funciona igual com duas gravações concorrentes.
const sqlInserirEvento = `
INSERT INTO evento (fonte, id_externo, tipo, versao_schema, quando, coletado_em, dados)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
ON CONFLICT (fonte, id_externo) DO NOTHING
RETURNING seq`

const sqlInserirOutbox = `INSERT INTO outbox (evento_seq) VALUES ($1)`

const sqlGravarCursor = `
INSERT INTO cursor_fonte (fonte, cursor, etag, atualizado_em)
VALUES ($1, $2, $3, now())
ON CONFLICT (fonte) DO UPDATE
SET cursor = EXCLUDED.cursor, etag = EXCLUDED.etag, atualizado_em = now()`

// Gravar grava as atividades novas, uma linha de outbox para cada uma e o cursor,
// tudo numa transação. Se qualquer passo falhar, nada fica gravado, nem o cursor:
// o próximo poll busca de novo e o dedup cuida do que se repetir.
// Devolve quantas atividades eram novas.
func (s *Store) Gravar(ctx context.Context, f atividade.Fonte, atividades []atividade.Atividade, cur fonte.Cursor) (int, error) {
	// Valida antes de abrir a transação: lote inválido não chega a tocar no banco.
	for _, a := range atividades {
		if a.Fonte != f {
			return 0, fmt.Errorf("eventstore: atividade da fonte %q num lote de %q", a.Fonte, f)
		}
		if err := a.Validar(); err != nil {
			return 0, fmt.Errorf("eventstore: %s: %w", a.IDExterno, err)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("eventstore: abrir transação: %w", err)
	}
	// Depois do Commit o Rollback não faz nada; antes dele, desfaz tudo.
	defer func() { _ = tx.Rollback(ctx) }()

	novas := 0
	for _, a := range atividades {
		var seq int64
		err := tx.QueryRow(ctx, sqlInserirEvento,
			string(a.Fonte), a.IDExterno, string(a.Tipo), a.VersaoSchema,
			a.Quando, a.ColetadoEm, string(a.Dados),
		).Scan(&seq)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // já estava gravado
		}
		if err != nil {
			return 0, fmt.Errorf("eventstore: inserir evento %s: %w", a.IDExterno, err)
		}
		if _, err := tx.Exec(ctx, sqlInserirOutbox, seq); err != nil {
			return 0, fmt.Errorf("eventstore: inserir outbox %d: %w", seq, err)
		}
		novas++
	}

	if _, err := tx.Exec(ctx, sqlGravarCursor, string(f), cur.Valor, cur.ETag); err != nil {
		return 0, fmt.Errorf("eventstore: gravar cursor de %s: %w", f, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("eventstore: commit: %w", err)
	}
	return novas, nil
}
