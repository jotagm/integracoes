package spotify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// URLToken é o endpoint de token do Spotify (fluxo authorization code).
const URLToken = "https://accounts.spotify.com/api/token"

// margemExpiracao renova o token um pouco antes de ele vencer,
// para uma requisição não sair com um token que expira no meio do caminho.
const margemExpiracao = time.Minute

// Credenciais do app e do usuário. O refresh token vem do fluxo OAuth que você
// já fez no spotify-top-artists; ele não expira sozinho, mas pode ser rotacionado.
type Credenciais struct {
	ClientID     string
	ClientSecret string
	RefreshToken string
}

// Tokens guarda o access token em memória e o renova quando precisa.
// Seguro para uso concorrente: várias goroutines pedindo ao mesmo tempo
// disparam uma renovação só.
type Tokens struct {
	urlToken string
	http     *http.Client
	agora    func() time.Time

	mu       sync.Mutex
	cred     Credenciais
	acesso   string
	expiraEm time.Time
}

// NovosTokens cria o gerenciador de tokens. urlToken vazio usa o endpoint real.
func NovosTokens(urlToken string, cred Credenciais) *Tokens {
	if urlToken == "" {
		urlToken = URLToken
	}
	return &Tokens{
		urlToken: urlToken,
		http:     &http.Client{Timeout: 15 * time.Second},
		agora:    time.Now,
		cred:     cred,
	}
}

// Acesso devolve um access token válido, renovando se o atual venceu ou não existe.
func (t *Tokens) Acesso(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.acesso != "" && t.agora().Before(t.expiraEm.Add(-margemExpiracao)) {
		return t.acesso, nil
	}
	if err := t.renovar(ctx); err != nil {
		return "", err
	}
	return t.acesso, nil
}

// Invalidar descarta o access token atual. O cliente chama isso quando recebe 401:
// o token pode ter sido revogado antes do prazo.
func (t *Tokens) Invalidar() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.acesso = ""
}

// RefreshToken devolve o refresh token atual. Se o Spotify rotacionar o token,
// este valor muda e precisa ser persistido, senão o próximo restart perde o acesso.
func (t *Tokens) RefreshToken() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cred.RefreshToken
}

type respostaToken struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

// renovar troca o refresh token por um access token novo. Chamar com t.mu travado.
func (t *Tokens) renovar(ctx context.Context) error {
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {t.cred.RefreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.urlToken, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("spotify: montar renovação: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(t.cred.ClientID, t.cred.ClientSecret)

	resp, err := t.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("spotify: renovação: %w", ctx.Err())
		}
		return fmt.Errorf("%w: renovação: %w", ErrTransitorio, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode >= 500:
		return fmt.Errorf("%w: renovação: status %d", ErrTransitorio, resp.StatusCode)
	default:
		// 400 invalid_grant: refresh token revogado ou errado. Precisa refazer o OAuth.
		return fmt.Errorf("%w: renovação: status %d", ErrPermanente, resp.StatusCode)
	}

	var r respostaToken
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil {
		return fmt.Errorf("%w: resposta de token ilegível: %w", ErrTransitorio, err)
	}
	if r.AccessToken == "" || r.ExpiresIn <= 0 {
		return fmt.Errorf("%w: resposta de token incompleta", ErrTransitorio)
	}

	t.acesso = r.AccessToken
	t.expiraEm = t.agora().Add(time.Duration(r.ExpiresIn) * time.Second)
	if r.RefreshToken != "" {
		t.cred.RefreshToken = r.RefreshToken
	}
	return nil
}
