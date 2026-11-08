package eventstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// As migrações vão dentro do binário: o deploy no lab não precisa copiar arquivos .sql.
//
//go:embed migrations/*.sql
var migracoes embed.FS

// Migrar aplica as migrações pendentes. Pode rodar em todo start: o que já foi aplicado é pulado.
func Migrar(ctx context.Context, pool *pgxpool.Pool) error {
	dir, err := fs.Sub(migracoes, "migrations")
	if err != nil {
		return fmt.Errorf("eventstore: abrir migrações: %w", err)
	}

	// O goose usa database/sql. Fechar este *sql.DB não fecha o pool do pgx.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	p, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		return fmt.Errorf("eventstore: preparar migrações: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("eventstore: aplicar migrações: %w", err)
	}
	return nil
}
