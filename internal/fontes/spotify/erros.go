// Package spotify coleta as músicas ouvidas recentemente pela Web API do Spotify.
//
// A API só guarda as últimas ~50 músicas. Se mais de 50 tocarem entre dois polls,
// as mais antigas se perdem: o pacote detecta isso e devolve uma LacunaDetectada
// em vez de fingir que o histórico está completo.
package spotify

import "github.com/jotagm/integracoes/internal/fonte"

// Mesmas classes de erro do GitHub, vindas do contrato comum.
var (
	// ErrTransitorio: vale tentar de novo com backoff (5xx, falha de rede).
	ErrTransitorio = fonte.ErrTransitorio
	// ErrPermanente: tentar de novo não resolve (refresh token revogado, 403, 404).
	ErrPermanente = fonte.ErrPermanente
)

// ErrLimite indica que o Spotify respondeu 429 e pediu para esperar.
type ErrLimite = fonte.ErrLimite
