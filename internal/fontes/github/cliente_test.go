package github

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// agoraFixo é a hora que o cliente enxerga nos testes de limite.
var agoraFixo = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

// servidor sobe um GitHub falso e devolve um cliente apontado para ele.
func servidor(t *testing.T, token string, h http.HandlerFunc) *Cliente {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := NovoCliente(srv.URL, token)
	c.agora = func() time.Time { return agoraFixo }
	return c
}

func TestPrimeiroPoll(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/users/jotagm/events/public" {
			t.Errorf("caminho errado: %s", r.URL.Path)
		}
		obrigatorios := map[string]string{
			"Accept":               "application/vnd.github+json",
			"X-GitHub-Api-Version": "2022-11-28",
			"User-Agent":           "integracoes",
		}
		for k, v := range obrigatorios {
			if got := r.Header.Get(k); got != v {
				t.Errorf("header %s: esperava %q, veio %q", k, v, got)
			}
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("sem token não deveria mandar Authorization")
		}
		if r.Header.Get("If-None-Match") != "" {
			t.Error("primeiro poll não deveria mandar If-None-Match")
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("X-Poll-Interval", "120")
		_, _ = w.Write([]byte(`[]`))
	})

	res, err := c.EventosPublicos(context.Background(), "jotagm", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.NaoMudou || string(res.Corpo) != `[]` || res.ETag != `"v1"` || res.IntervaloPoll != 120*time.Second {
		t.Fatalf("resultado inesperado: %+v", res)
	}
}

func TestTokenVaiNoAuthorization(t *testing.T) {
	c := servidor(t, "segredo", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer segredo" {
			t.Errorf("Authorization: veio %q", got)
		}
		_, _ = w.Write([]byte(`[]`))
	})
	if _, err := c.EventosPublicos(context.Background(), "jotagm", ""); err != nil {
		t.Fatal(err)
	}
}

func TestUsuarioEscapadoNoCaminho(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.EscapedPath(); got != "/users/a%2Fb/events/public" {
			t.Errorf("caminho escapado: veio %s", got)
		}
		_, _ = w.Write([]byte(`[]`))
	})
	if _, err := c.EventosPublicos(context.Background(), "a/b", ""); err != nil {
		t.Fatal(err)
	}
}

func TestPollCondicional304(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-None-Match"); got != `"v1"` {
			t.Errorf("If-None-Match: veio %q", got)
		}
		w.WriteHeader(http.StatusNotModified)
	})

	res, err := c.EventosPublicos(context.Background(), "jotagm", `"v1"`)
	if err != nil {
		t.Fatal(err)
	}
	if !res.NaoMudou || res.Corpo != nil || res.ETag != `"v1"` {
		t.Fatalf("304 deveria manter o ETag anterior e vir sem corpo: %+v", res)
	}
}

func TestIntervaloDePoll(t *testing.T) {
	casos := []struct {
		header string
		quer   time.Duration
	}{
		{"30", 30 * time.Second},
		{"", intervaloPadrao},
		{"abc", intervaloPadrao},
		{"0", intervaloPadrao},
		{"-5", intervaloPadrao},
	}
	for _, caso := range casos {
		t.Run("header="+caso.header, func(t *testing.T) {
			c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
				if caso.header != "" {
					w.Header().Set("X-Poll-Interval", caso.header)
				}
				_, _ = w.Write([]byte(`[]`))
			})
			res, err := c.EventosPublicos(context.Background(), "jotagm", "")
			if err != nil {
				t.Fatal(err)
			}
			if res.IntervaloPoll != caso.quer {
				t.Fatalf("esperava %s, veio %s", caso.quer, res.IntervaloPoll)
			}
		})
	}
}

func TestLimite(t *testing.T) {
	reset := func(d time.Duration) string { return strconv.FormatInt(agoraFixo.Add(d).Unix(), 10) }

	casos := []struct {
		nome    string
		status  int
		headers map[string]string
		quer    time.Duration
	}{
		{"429 com Retry-After", 429, map[string]string{"Retry-After": "30"}, 30 * time.Second},
		{"403 com Retry-After", 403, map[string]string{"Retry-After": "10"}, 10 * time.Second},
		{"403 com quota zerada usa o reset", 403, map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": reset(90 * time.Second)}, 90 * time.Second},
		{"Retry-After vence o reset", 429, map[string]string{"Retry-After": "5", "X-RateLimit-Reset": reset(time.Hour)}, 5 * time.Second},
		{"reset no passado cai no padrão", 403, map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": reset(-time.Minute)}, esperaPadrao},
		{"429 sem headers cai no padrão", 429, nil, esperaPadrao},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
				for k, v := range caso.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(caso.status)
			})
			_, err := c.EventosPublicos(context.Background(), "jotagm", "")
			var lim *ErrLimite
			if !errors.As(err, &lim) {
				t.Fatalf("esperava *ErrLimite, veio %v", err)
			}
			if lim.RetryAfter != caso.quer {
				t.Fatalf("espera: esperava %s, veio %s", caso.quer, lim.RetryAfter)
			}
		})
	}
}

func TestClassesDeErro(t *testing.T) {
	casos := []struct {
		status int
		quer   error
	}{
		{500, ErrTransitorio},
		{502, ErrTransitorio},
		{503, ErrTransitorio},
		{401, ErrPermanente},
		{404, ErrPermanente},
		{422, ErrPermanente},
		{403, ErrPermanente}, // 403 sem sinal de limite: falta de permissão, não quota
	}
	for _, caso := range casos {
		t.Run(strconv.Itoa(caso.status), func(t *testing.T) {
			c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(caso.status)
			})
			_, err := c.EventosPublicos(context.Background(), "jotagm", "")
			if !errors.Is(err, caso.quer) {
				t.Fatalf("esperava %v, veio %v", caso.quer, err)
			}
			var lim *ErrLimite
			if errors.As(err, &lim) {
				t.Fatal("não deveria ser tratado como limite")
			}
		})
	}
}

func TestCorpoMaiorQueOLimite(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", limiteCorpo+1)))
	})
	_, err := c.EventosPublicos(context.Background(), "jotagm", "")
	if !errors.Is(err, ErrPermanente) {
		t.Fatalf("corpo cortado deveria ser erro permanente, veio %v", err)
	}
}

func TestCorpoExatamenteNoLimite(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", limiteCorpo)))
	})
	res, err := c.EventosPublicos(context.Background(), "jotagm", "")
	if err != nil || len(res.Corpo) != limiteCorpo {
		t.Fatalf("corpo no limite deveria passar inteiro: len=%d err=%v", len(res.Corpo), err)
	}
}

func TestFalhaDeRedeETransitoria(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close() // nada escutando: a conexão é recusada
	c := NovoCliente(srv.URL, "")

	_, err := c.EventosPublicos(context.Background(), "jotagm", "")
	if !errors.Is(err, ErrTransitorio) {
		t.Fatalf("falha de rede deveria ser transitória, veio %v", err)
	}
}

func TestContextoCanceladoNaoETransitorio(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()

	_, err := c.EventosPublicos(ctx, "jotagm", "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperava context.Canceled, veio %v", err)
	}
	if errors.Is(err, ErrTransitorio) {
		t.Fatal("cancelamento não deveria virar erro transitório")
	}
}
