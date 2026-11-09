// Package espera reúne o que todo laço de retentativa precisa: dormir respeitando
// o cancelamento e calcular o backoff exponencial com jitter.
package espera

import (
	"context"
	"math/rand/v2"
	"time"
)

// Esperar dorme d ou volta antes, com o erro do contexto, se ele for cancelado.
func Esperar(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Backoff dobra a espera a cada falha seguida, de Min até Max, com jitter.
type Backoff struct {
	Min, Max time.Duration
	// Jitter espalha as tentativas. Nil usa MeioJitter; os testes passam a identidade.
	Jitter func(time.Duration) time.Duration
}

// Padrao é o backoff de 1 s a 5 min usado pelo ingestor e pelo relay.
func Padrao() Backoff {
	return Backoff{Min: time.Second, Max: 5 * time.Minute, Jitter: MeioJitter}
}

// Duracao devolve quanto esperar depois de `falhas` falhas seguidas (a partir de 1).
func (b Backoff) Duracao(falhas int) time.Duration {
	d := b.Min
	for n := 1; n < falhas && d < b.Max; n++ {
		d *= 2
	}
	d = min(d, b.Max)
	if b.Jitter == nil {
		return MeioJitter(d)
	}
	return b.Jitter(d)
}

// MeioJitter devolve um valor entre d/2 e d: espalha as tentativas sem nunca esperar quase zero.
func MeioJitter(d time.Duration) time.Duration {
	metade := d / 2
	return metade + rand.N(metade+1)
}
