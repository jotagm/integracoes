package spotify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// tokenFalso conta quantas vezes o cliente invalidou e devolve um token por "geração".
type tokenFalso struct {
	geracao      atomic.Int32
	invalidacoes atomic.Int32
	err          error
}

func (f *tokenFalso) Acesso(context.Context) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return fmt.Sprintf("token-%d", f.geracao.Load()), nil
}

func (f *tokenFalso) Invalidar() {
	f.invalidacoes.Add(1)
	f.geracao.Add(1)
}

func servidorAPI(t *testing.T, tk fonteDeToken, h http.HandlerFunc) *Cliente {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NovoCliente(srv.URL, tk)
}

func TestPrimeiroPollSemCursor(t *testing.T) {
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/me/player/recently-played" {
			t.Errorf("caminho: %s", r.URL.Path)
		}
		if r.URL.Query().Get("limit") != "50" || r.URL.Query().Has("after") {
			t.Errorf("query: %s", r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-0" {
			t.Errorf("authorization: %s", got)
		}
		fmt.Fprint(w, `{"items":[]}`)
	})
	corpo, err := c.TocadasRecentemente(context.Background(), time.Time{})
	if err != nil || string(corpo) != `{"items":[]}` {
		t.Fatalf("corpo=%s err=%v", corpo, err)
	}
}

func TestCursorViraAfterEmMilissegundos(t *testing.T) {
	cursor := time.Date(2026, 11, 9, 21, 14, 3, 123_000_000, time.UTC)
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("after"); got != fmt.Sprint(cursor.UnixMilli()) {
			t.Errorf("after: veio %s", got)
		}
		fmt.Fprint(w, `{"items":[]}`)
	})
	if _, err := c.TocadasRecentemente(context.Background(), cursor); err != nil {
		t.Fatal(err)
	}
}

func Test401RenovaETentaUmaVez(t *testing.T) {
	tk := &tokenFalso{}
	var chamadas atomic.Int32
	c := servidorAPI(t, tk, func(w http.ResponseWriter, r *http.Request) {
		chamadas.Add(1)
		if r.Header.Get("Authorization") == "Bearer token-0" {
			w.WriteHeader(http.StatusUnauthorized) // o primeiro token foi revogado
			return
		}
		fmt.Fprint(w, `{"items":[]}`)
	})

	if _, err := c.TocadasRecentemente(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if chamadas.Load() != 2 || tk.invalidacoes.Load() != 1 {
		t.Fatalf("esperava 2 chamadas e 1 invalidação, veio %d e %d", chamadas.Load(), tk.invalidacoes.Load())
	}
}

func Test401DuasVezesEPermanente(t *testing.T) {
	var chamadas atomic.Int32
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		chamadas.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})
	_, err := c.TocadasRecentemente(context.Background(), time.Time{})
	if !errors.Is(err, ErrPermanente) {
		t.Fatalf("esperava ErrPermanente, veio %v", err)
	}
	if chamadas.Load() != 2 {
		t.Fatalf("deveria tentar só uma vez a mais, chamou %d", chamadas.Load())
	}
}

func Test429ViraErrLimite(t *testing.T) {
	casos := []struct {
		retryAfter string
		quer       time.Duration
	}{
		{"7", 7 * time.Second},
		{"", esperaPadrao},
		{"abc", esperaPadrao},
	}
	for _, caso := range casos {
		t.Run("retry-after="+caso.retryAfter, func(t *testing.T) {
			c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
				if caso.retryAfter != "" {
					w.Header().Set("Retry-After", caso.retryAfter)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			})
			_, err := c.TocadasRecentemente(context.Background(), time.Time{})
			var lim *ErrLimite
			if !errors.As(err, &lim) || lim.RetryAfter != caso.quer {
				t.Fatalf("esperava ErrLimite de %s, veio %v", caso.quer, err)
			}
		})
	}
}

func TestClassesDeErroDaAPI(t *testing.T) {
	casos := []struct {
		status int
		quer   error
	}{
		{500, ErrTransitorio},
		{502, ErrTransitorio},
		{403, ErrPermanente}, // escopo user-read-recently-played faltando
		{404, ErrPermanente},
	}
	for _, caso := range casos {
		t.Run(fmt.Sprint(caso.status), func(t *testing.T) {
			c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(caso.status)
			})
			_, err := c.TocadasRecentemente(context.Background(), time.Time{})
			if !errors.Is(err, caso.quer) {
				t.Fatalf("esperava %v, veio %v", caso.quer, err)
			}
		})
	}
}

func TestErroDoTokenSobeSemChamarAAPI(t *testing.T) {
	var chamadas atomic.Int32
	c := servidorAPI(t, &tokenFalso{err: fmt.Errorf("%w: invalid_grant", ErrPermanente)}, func(w http.ResponseWriter, r *http.Request) {
		chamadas.Add(1)
	})
	_, err := c.TocadasRecentemente(context.Background(), time.Time{})
	if !errors.Is(err, ErrPermanente) || chamadas.Load() != 0 {
		t.Fatalf("err=%v chamadas=%d", err, chamadas.Load())
	}
}

func TestContextoCanceladoNaoETransitorioNoSpotify(t *testing.T) {
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[]}`)
	})
	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()
	_, err := c.TocadasRecentemente(ctx, time.Time{})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrTransitorio) {
		t.Fatalf("esperava só context.Canceled, veio %v", err)
	}
}
