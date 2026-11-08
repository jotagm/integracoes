// Package fonte é o contrato entre o ingestor e cada integração (porta, na
// arquitetura hexagonal). O ingestor só conhece este pacote; GitHub e Spotify
// são adaptadores que o implementam.
package fonte

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
)

// Cursor é onde a fonte parou. Cada fonte usa o campo que faz sentido:
// o GitHub usa ETag, o Spotify usa Valor (o played_at mais recente).
// É gravado na mesma transação dos eventos.
type Cursor struct {
	Valor string
	ETag  string
}

// Coleta é o resultado de um poll.
type Coleta struct {
	Atividades  []atividade.Atividade
	NovoCursor  Cursor
	ProximoPoll time.Duration
	Descartes   int
}

// Fonte é uma integração que o ingestor consulta em laço.
type Fonte interface {
	Nome() atividade.Fonte
	Coletar(ctx context.Context, cursor Cursor) (Coleta, error)
}

// Classes de erro que todas as fontes devolvem (embrulhadas). O ingestor decide
// o que fazer só por elas, sem saber de qual integração o erro veio.
var (
	// ErrTransitorio: tentar de novo com backoff (5xx, rede, resposta ilegível).
	ErrTransitorio = errors.New("erro transitório")
	// ErrPermanente: parar a fonte; tentar de novo não resolve (credencial revogada, 404).
	ErrPermanente = errors.New("erro permanente")
)

// ErrLimite: a fonte pediu para esperar um tempo definido antes de tentar de novo.
type ErrLimite struct {
	RetryAfter time.Duration
}

func (e *ErrLimite) Error() string {
	return fmt.Sprintf("limite atingido, tentar após %s", e.RetryAfter)
}
