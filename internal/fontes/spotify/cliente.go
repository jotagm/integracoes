package spotify

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	// URLAPI é a base da Web API do Spotify.
	URLAPI = "https://api.spotify.com/v1"
	// limiteItens é o máximo que o endpoint aceita, e também o tamanho da janela que ele guarda.
	limiteItens  = 50
	limiteCorpo  = 2 << 20 // 50 músicas cabem com folga em 2 MiB
	esperaPadrao = 60 * time.Second
)

// fonteDeToken é o que o cliente precisa dos Tokens. A interface deixa o teste
// usar um token falso sem subir servidor de OAuth.
type fonteDeToken interface {
	Acesso(ctx context.Context) (string, error)
	Invalidar()
}

// Cliente busca as músicas ouvidas recentemente.
type Cliente struct {
	urlAPI string
	tokens fonteDeToken
	http   *http.Client
}

// NovoCliente cria o cliente. urlAPI vazio usa a API real.
func NovoCliente(urlAPI string, tokens fonteDeToken) *Cliente {
	if urlAPI == "" {
		urlAPI = URLAPI
	}
	return &Cliente{
		urlAPI: urlAPI,
		tokens: tokens,
		http:   &http.Client{Timeout: 15 * time.Second},
	}
}

// TocadasRecentemente devolve o corpo cru da resposta, para a normalização tratar.
// depoisDe zero significa primeiro poll: traz as últimas 50 sem cursor.
func (c *Cliente) TocadasRecentemente(ctx context.Context, depoisDe time.Time) ([]byte, error) {
	q := url.Values{"limit": {strconv.Itoa(limiteItens)}}
	if !depoisDe.IsZero() {
		q.Set("after", strconv.FormatInt(depoisDe.UnixMilli(), 10))
	}
	endereco := c.urlAPI + "/me/player/recently-played?" + q.Encode()

	corpo, status, err := c.get(ctx, endereco)
	if err == nil && status == http.StatusUnauthorized {
		// Token revogado antes do prazo: descarta, renova e tenta uma única vez.
		c.tokens.Invalidar()
		corpo, status, err = c.get(ctx, endereco)
	}
	if err != nil {
		return nil, err
	}
	if status == http.StatusUnauthorized {
		return nil, fmt.Errorf("%w: 401 mesmo depois de renovar o token", ErrPermanente)
	}
	return corpo, nil
}

// get faz uma requisição autenticada. Devolve o status 401 sem erro para o chamador
// decidir se renova; os demais status viram erro classificado.
func (c *Cliente) get(ctx context.Context, endereco string) ([]byte, int, error) {
	token, err := c.tokens.Acesso(ctx)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endereco, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("spotify: montar requisição: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, fmt.Errorf("spotify: requisição: %w", ctx.Err())
		}
		return nil, 0, fmt.Errorf("%w: requisição: %w", ErrTransitorio, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, limiteCorpo))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusOK:
		corpo, err := io.ReadAll(io.LimitReader(resp.Body, limiteCorpo+1))
		if err != nil {
			return nil, 0, fmt.Errorf("%w: ler corpo: %w", ErrTransitorio, err)
		}
		if len(corpo) > limiteCorpo {
			return nil, 0, fmt.Errorf("%w: corpo maior que %d bytes", ErrPermanente, limiteCorpo)
		}
		return corpo, resp.StatusCode, nil

	case resp.StatusCode == http.StatusUnauthorized:
		return nil, resp.StatusCode, nil

	case resp.StatusCode == http.StatusTooManyRequests:
		espera := esperaPadrao
		if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 {
			espera = time.Duration(n) * time.Second
		}
		return nil, 0, &ErrLimite{RetryAfter: espera}

	case resp.StatusCode >= 500:
		return nil, 0, fmt.Errorf("%w: status %d", ErrTransitorio, resp.StatusCode)

	default:
		return nil, 0, fmt.Errorf("%w: status %d", ErrPermanente, resp.StatusCode)
	}
}
