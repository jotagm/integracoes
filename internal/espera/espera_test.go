package espera

import (
	"context"
	"errors"
	"testing"
	"time"
)

func semJitter() Backoff {
	return Backoff{Min: time.Second, Max: 5 * time.Minute, Jitter: func(d time.Duration) time.Duration { return d }}
}

func TestBackoffDobra(t *testing.T) {
	b := semJitter()
	quer := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for i, q := range quer {
		if d := b.Duracao(i + 1); d != q {
			t.Fatalf("falha %d: esperava %s, veio %s", i+1, q, d)
		}
	}
}

func TestBackoffTemTeto(t *testing.T) {
	b := semJitter()
	// 1s dobrando: a 9ª falha espera 2^8 s = 4m16s; a 10ª já passaria de 5 min e fica no teto.
	if d := b.Duracao(9); d != 256*time.Second {
		t.Fatalf("9ª falha: esperava 4m16s, veio %s", d)
	}
	for _, falhas := range []int{10, 11, 50, 1000} {
		if d := b.Duracao(falhas); d != b.Max {
			t.Fatalf("com %d falhas deveria ficar no teto %s, veio %s", falhas, b.Max, d)
		}
	}
}

func TestBackoffSemJitterDefinidoUsaMeioJitter(t *testing.T) {
	b := Backoff{Min: 10 * time.Second, Max: time.Minute}
	for range 100 {
		if d := b.Duracao(1); d < 5*time.Second || d > 10*time.Second {
			t.Fatalf("fora da faixa: %s", d)
		}
	}
}

func TestMeioJitterFicaEntreMetadeEOTotal(t *testing.T) {
	for range 1000 {
		d := MeioJitter(10 * time.Second)
		if d < 5*time.Second || d > 10*time.Second {
			t.Fatalf("fora da faixa: %s", d)
		}
	}
}

func TestEsperarRespeitaOContexto(t *testing.T) {
	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()
	if err := Esperar(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("com contexto cancelado deveria voltar na hora, veio %v", err)
	}
	if err := Esperar(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}
