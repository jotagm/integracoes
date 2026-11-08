package spotify

import (
	"context"
	"fmt"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// IntervaloPadrao é de quanto em quanto tempo o Spotify é consultado.
// Precisa ser curto o bastante para menos de 50 músicas tocarem entre dois polls.
const IntervaloPadrao = 10 * time.Minute

// Fonte adapta o cliente e a normalização do Spotify ao contrato fonte.Fonte.
// O cursor é o played_at mais recente, em RFC 3339 com nanossegundos, no campo Valor.
type Fonte struct {
	cliente   *Cliente
	intervalo time.Duration
	agora     func() time.Time
}

// NovaFonte cria a fonte. intervalo zero usa IntervaloPadrao.
func NovaFonte(c *Cliente, intervalo time.Duration) *Fonte {
	if intervalo <= 0 {
		intervalo = IntervaloPadrao
	}
	return &Fonte{cliente: c, intervalo: intervalo, agora: time.Now}
}

// Nome identifica a fonte no event store e no Kafka.
func (f *Fonte) Nome() atividade.Fonte { return atividade.FonteSpotify }

// Coletar busca o que tocou depois do cursor e normaliza, incluindo a lacuna se houver.
func (f *Fonte) Coletar(ctx context.Context, cur fonte.Cursor) (fonte.Coleta, error) {
	var cursorAnterior time.Time
	if cur.Valor != "" {
		t, err := time.Parse(time.RFC3339Nano, cur.Valor)
		if err != nil {
			// Cursor corrompido no banco: tentar de novo daria o mesmo erro.
			return fonte.Coleta{}, fmt.Errorf("%w: cursor ilegível %q: %w", fonte.ErrPermanente, cur.Valor, err)
		}
		cursorAnterior = t
	}

	corpo, err := f.cliente.TocadasRecentemente(ctx, cursorAnterior)
	if err != nil {
		return fonte.Coleta{}, err
	}

	lote, err := NormalizarLote(corpo, cursorAnterior, f.agora())
	if err != nil {
		return fonte.Coleta{}, fmt.Errorf("%w: %w", fonte.ErrTransitorio, err)
	}

	coleta := fonte.Coleta{
		Atividades:  lote.Atividades,
		Descartes:   len(lote.Descartes),
		ProximoPoll: f.intervalo,
		NovoCursor:  cur,
	}
	if !lote.NovoCursor.IsZero() {
		coleta.NovoCursor = fonte.Cursor{Valor: lote.NovoCursor.UTC().Format(time.RFC3339Nano)}
	}
	return coleta, nil
}
