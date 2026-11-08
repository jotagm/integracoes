package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jotagm/integracoes/internal/eventstore"
	"github.com/jotagm/integracoes/internal/fonte"
	"github.com/jotagm/integracoes/internal/fontes/github"
	"github.com/jotagm/integracoes/internal/fontes/spotify"
	"github.com/jotagm/integracoes/internal/ingestor"
)

const urlAPIGitHub = "https://api.github.com"

// rodarIngestor sobe o event store, monta as fontes configuradas e roda até o SIGTERM.
//
// Configuração por variável de ambiente (veja .env.example):
//
//	DATABASE_URL                       obrigatório
//	GITHUB_USUARIO, GITHUB_TOKEN       liga a fonte do GitHub (token opcional)
//	SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN
//	                                   liga a fonte do Spotify (as três juntas)
func rodarIngestor(ctx context.Context, _ []string) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("variável DATABASE_URL não definida")
	}

	fontes, err := fontesConfiguradas()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("abrir pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("conectar no postgres: %w", err)
	}
	if err := eventstore.Migrar(ctx, pool); err != nil {
		return err
	}

	ingestor.Novo(eventstore.Novo(pool), slog.Default()).Rodar(ctx, fontes...)
	return nil
}

func fontesConfiguradas() ([]fonte.Fonte, error) {
	var fontes []fonte.Fonte

	if usuario := os.Getenv("GITHUB_USUARIO"); usuario != "" {
		c := github.NovoCliente(urlAPIGitHub, os.Getenv("GITHUB_TOKEN"))
		fontes = append(fontes, github.NovaFonte(c, usuario))
	}

	cred := spotify.Credenciais{
		ClientID:     os.Getenv("SPOTIFY_CLIENT_ID"),
		ClientSecret: os.Getenv("SPOTIFY_CLIENT_SECRET"),
		RefreshToken: os.Getenv("SPOTIFY_REFRESH_TOKEN"),
	}
	definidas := 0
	for _, v := range []string{cred.ClientID, cred.ClientSecret, cred.RefreshToken} {
		if v != "" {
			definidas++
		}
	}
	switch definidas {
	case 0:
	case 3:
		tokens := spotify.NovosTokens("", cred)
		fontes = append(fontes, spotify.NovaFonte(spotify.NovoCliente("", tokens), 0))
	default:
		return nil, errors.New("spotify pela metade: defina SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET e SPOTIFY_REFRESH_TOKEN juntas")
	}

	if len(fontes) == 0 {
		return nil, errors.New("nenhuma fonte configurada: defina GITHUB_USUARIO e/ou as variáveis do Spotify")
	}
	return fontes, nil
}
