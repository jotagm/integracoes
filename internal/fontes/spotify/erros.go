// Package spotify coleta as músicas ouvidas recentemente pela Web API do Spotify.
//
// A API só guarda as últimas ~50 músicas. Se mais de 50 tocarem entre dois polls,
// as mais antigas se perdem: o pacote detecta isso e devolve uma LacunaDetectada
// em vez de fingir que o histórico está completo.
package spotify

import (
	"errors"
	"fmt"
	"time"
)

// Mesmas classes de erro da fonte do GitHub, para o ingestor tratar as fontes igual.
var (
	// ErrTransitorio: vale tentar de novo com backoff (5xx, falha de rede).
	ErrTransitorio = errors.New("spotify: erro transitório")
	// ErrPermanente: tentar de novo não resolve (refresh token revogado, 403, 404).
	ErrPermanente = errors.New("spotify: erro permanente")
)

// ErrLimite indica que o Spotify respondeu 429 e pediu para esperar.
type ErrLimite struct {
	RetryAfter time.Duration
}

func (e *ErrLimite) Error() string {
	return fmt.Sprintf("spotify: limite atingido, tentar após %s", e.RetryAfter)
}
