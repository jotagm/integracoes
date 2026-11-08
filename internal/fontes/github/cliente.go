package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jotagm/integracoes/internal/fonte"
)

const (
	userAgent       = "integracoes"
	intervaloPadrao = 60 * time.Second
	esperaPadrao    = 60 * time.Second
	limiteCorpo     = 5 << 20 // 5 MiB: nunca confie no tamanho que vem de fora
)

// Cliente busca eventos públicos de um usuário do GitHub.
// A URL base é um campo para o teste poder trocar pela do httptest.
type Cliente struct {
	urlBase string
	token   string
	http    *http.Client
	agora   func() time.Time // relógio injetável: o teste fixa a hora
}

// NovoCliente cria um Cliente com timeout. Token vazio significa sem autenticação.
func NovoCliente(urlBase, token string) *Cliente {
	return &Cliente{
		urlBase: urlBase,
		token:   token,
		http:    &http.Client{Timeout: 15 * time.Second},
		agora:   time.Now,
	}
}

// Resultado é tudo que o ingestor precisa para decidir o próximo poll.
type Resultado struct {
	Corpo         []byte
	ETag          string
	NaoMudou      bool
	IntervaloPoll time.Duration
}

// As classes de erro vêm do contrato comum (pacote fonte). Os aliases mantêm
// os nomes locais: errors.Is(err, github.ErrTransitorio) e
// errors.Is(err, fonte.ErrTransitorio) são a mesma verificação.
var (
	// ErrTransitorio: vale tentar de novo com backoff (5xx, falha de rede).
	ErrTransitorio = fonte.ErrTransitorio
	// ErrPermanente: tentar de novo não resolve (401, 404, corpo grande demais).
	ErrPermanente = fonte.ErrPermanente
)

// ErrLimite indica que o GitHub pediu para esperar antes de tentar de novo.
type ErrLimite = fonte.ErrLimite

// EventosPublicos faz um poll. etagAnterior vazio significa primeiro poll.
func (c *Cliente) EventosPublicos(ctx context.Context, usuario, etagAnterior string) (Resultado, error) {
	// per_page=100 é o máximo: com o padrão de 30, um dia movimentado perderia eventos entre polls.
	endereco := c.urlBase + "/users/" + url.PathEscape(usuario) + "/events/public?per_page=100"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endereco, nil)
	if err != nil {
		return Resultado{}, fmt.Errorf("github: montar requisição: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if etagAnterior != "" {
		req.Header.Set("If-None-Match", etagAnterior)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// Cancelamento é decisão de quem chamou, não falha da fonte: não vira transitório.
		if ctx.Err() != nil {
			return Resultado{}, fmt.Errorf("github: requisição: %w", ctx.Err())
		}
		return Resultado{}, fmt.Errorf("%w: requisição: %w", ErrTransitorio, err)
	}
	defer func() {
		// Esvaziar o corpo antes de fechar deixa a conexão ser reaproveitada.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, limiteCorpo))
		_ = resp.Body.Close()
	}()

	intervalo := lerSegundos(resp.Header.Get("X-Poll-Interval"), intervaloPadrao)

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return Resultado{ETag: etagAnterior, NaoMudou: true, IntervaloPoll: intervalo}, nil

	case resp.StatusCode == http.StatusOK:
		// Lê um byte a mais que o limite: se ele vier, o corpo foi cortado e o JSON estaria quebrado.
		corpo, err := io.ReadAll(io.LimitReader(resp.Body, limiteCorpo+1))
		if err != nil {
			return Resultado{}, fmt.Errorf("%w: ler corpo: %w", ErrTransitorio, err)
		}
		if len(corpo) > limiteCorpo {
			return Resultado{}, fmt.Errorf("%w: corpo maior que %d bytes", ErrPermanente, limiteCorpo)
		}
		return Resultado{Corpo: corpo, ETag: resp.Header.Get("ETag"), IntervaloPoll: intervalo}, nil

	case limiteAtingido(resp):
		return Resultado{}, &ErrLimite{RetryAfter: esperaDoLimite(resp, c.agora())}

	case resp.StatusCode >= 500:
		return Resultado{}, fmt.Errorf("%w: status %d", ErrTransitorio, resp.StatusCode)

	default:
		return Resultado{}, fmt.Errorf("%w: status %d", ErrPermanente, resp.StatusCode)
	}
}

// limiteAtingido cobre o 429 e o 403 que o GitHub usa para limite de requisições.
func limiteAtingido(resp *http.Response) bool {
	if resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return resp.StatusCode == http.StatusForbidden &&
		(resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-RateLimit-Remaining") == "0")
}

// esperaDoLimite usa o Retry-After; sem ele, o horário de reset da quota; sem os dois, um padrão.
func esperaDoLimite(resp *http.Response, agora time.Time) time.Duration {
	if d := lerSegundos(resp.Header.Get("Retry-After"), 0); d > 0 {
		return d
	}
	if reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
		if d := time.Unix(reset, 0).Sub(agora); d > 0 {
			return d
		}
	}
	return esperaPadrao
}

// lerSegundos converte um header em segundos; valor ausente ou inválido vira o padrão.
func lerSegundos(valor string, padrao time.Duration) time.Duration {
	n, err := strconv.Atoi(valor)
	if err != nil || n <= 0 {
		return padrao
	}
	return time.Duration(n) * time.Second
}
