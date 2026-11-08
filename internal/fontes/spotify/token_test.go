package spotify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// relogio é um relógio de teste que só anda quando o teste manda.
type relogio struct {
	mu sync.Mutex
	t  time.Time
}

func (r *relogio) agora() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.t
}

func (r *relogio) avancar(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.t = r.t.Add(d)
}

// servidorToken sobe um endpoint de token falso que conta as renovações.
func servidorToken(t *testing.T, h http.HandlerFunc) (*Tokens, *relogio) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	r := &relogio{t: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	tk := NovosTokens(srv.URL, Credenciais{ClientID: "id", ClientSecret: "segredo", RefreshToken: "refresh-1"})
	tk.agora = r.agora
	return tk, r
}

func TestRenovacaoMandaOQueOSpotifyPede(t *testing.T) {
	tk, _ := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("método: %s", r.Method)
		}
		if u, s, ok := r.BasicAuth(); !ok || u != "id" || s != "segredo" {
			t.Errorf("basic auth: %q %q %v", u, s, ok)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
			t.Errorf("content-type: %s", ct)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.PostForm.Get("grant_type") != "refresh_token" || r.PostForm.Get("refresh_token") != "refresh-1" {
			t.Errorf("form: %v", r.PostForm)
		}
		fmt.Fprint(w, `{"access_token":"acesso-1","token_type":"Bearer","expires_in":3600}`)
	})

	got, err := tk.Acesso(context.Background())
	if err != nil || got != "acesso-1" {
		t.Fatalf("esperava acesso-1, veio %q (%v)", got, err)
	}
}

func TestTokenEmCacheAteQuaseVencer(t *testing.T) {
	var renovacoes atomic.Int32
	tk, rel := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
		n := renovacoes.Add(1)
		fmt.Fprintf(w, `{"access_token":"acesso-%d","expires_in":3600}`, n)
	})
	ctx := context.Background()

	primeiro, _ := tk.Acesso(ctx)
	rel.avancar(58 * time.Minute) // ainda fora da margem de 1 minuto
	segundo, _ := tk.Acesso(ctx)
	if primeiro != "acesso-1" || segundo != "acesso-1" || renovacoes.Load() != 1 {
		t.Fatalf("deveria reusar o token: %s %s, renovações=%d", primeiro, segundo, renovacoes.Load())
	}

	rel.avancar(90 * time.Second) // agora dentro da margem
	terceiro, _ := tk.Acesso(ctx)
	if terceiro != "acesso-2" || renovacoes.Load() != 2 {
		t.Fatalf("deveria renovar perto do vencimento: %s, renovações=%d", terceiro, renovacoes.Load())
	}
}

func TestInvalidarForcaRenovacao(t *testing.T) {
	var renovacoes atomic.Int32
	tk, _ := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
		n := renovacoes.Add(1)
		fmt.Fprintf(w, `{"access_token":"acesso-%d","expires_in":3600}`, n)
	})
	ctx := context.Background()

	_, _ = tk.Acesso(ctx)
	tk.Invalidar()
	got, _ := tk.Acesso(ctx)
	if got != "acesso-2" {
		t.Fatalf("depois de invalidar deveria renovar, veio %s", got)
	}
}

func TestRefreshTokenRotacionado(t *testing.T) {
	tk, _ := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"a","expires_in":3600,"refresh_token":"refresh-2"}`)
	})
	if _, err := tk.Acesso(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := tk.RefreshToken(); got != "refresh-2" {
		t.Fatalf("deveria guardar o refresh token novo, veio %s", got)
	}
}

func TestRenovacoesConcorrentesViramUma(t *testing.T) {
	var renovacoes atomic.Int32
	tk, _ := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
		renovacoes.Add(1)
		time.Sleep(20 * time.Millisecond) // segura a renovação para as goroutines se acumularem
		fmt.Fprint(w, `{"access_token":"a","expires_in":3600}`)
	})

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := tk.Acesso(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := renovacoes.Load(); n != 1 {
		t.Fatalf("20 pedidos simultâneos deveriam renovar 1 vez, renovaram %d", n)
	}
}

func TestErrosDeRenovacao(t *testing.T) {
	casos := []struct {
		nome   string
		status int
		corpo  string
		quer   error
	}{
		{"refresh revogado", 400, `{"error":"invalid_grant"}`, ErrPermanente},
		{"credencial do app errada", 401, `{"error":"invalid_client"}`, ErrPermanente},
		{"spotify fora do ar", 503, ``, ErrTransitorio},
		{"resposta ilegível", 200, `{`, ErrTransitorio},
		{"resposta sem token", 200, `{"expires_in":3600}`, ErrTransitorio},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			tk, _ := servidorToken(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(c.status)
				fmt.Fprint(w, c.corpo)
			})
			_, err := tk.Acesso(context.Background())
			if !errors.Is(err, c.quer) {
				t.Fatalf("esperava %v, veio %v", c.quer, err)
			}
		})
	}
}
