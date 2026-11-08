package spotify

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
)

type resposta struct {
	Itens []item `json:"items"`
}

type item struct {
	TocadaEm *time.Time `json:"played_at"`
	Musica   musica     `json:"track"`
}

type musica struct {
	ID        string    `json:"id"`
	Nome      string    `json:"name"`
	DuracaoMs int       `json:"duration_ms"`
	Artistas  []artista `json:"artists"`
	Album     album     `json:"album"`
}

type artista struct {
	Nome string `json:"name"`
}

type album struct {
	Nome string `json:"name"`
}

// Descarte registra um item que não virou atividade e por quê.
type Descarte struct {
	ID     string
	Motivo string
}

// Lote é o resultado de normalizar uma resposta.
type Lote struct {
	Atividades []atividade.Atividade
	Descartes  []Descarte
	// NovoCursor é o played_at mais recente visto; sem itens, repete o cursor anterior.
	// O ingestor grava este valor na mesma transação dos eventos.
	NovoCursor time.Time
}

// NormalizarLote transforma a resposta em atividades e, se for o caso, numa LacunaDetectada.
// cursorAnterior é o played_at mais recente já gravado; zero no primeiro poll.
func NormalizarLote(corpo []byte, cursorAnterior, coletadoEm time.Time) (Lote, error) {
	var r resposta
	if err := json.Unmarshal(corpo, &r); err != nil {
		return Lote{}, fmt.Errorf("spotify: corpo ilegível: %w", err)
	}

	lote := Lote{NovoCursor: cursorAnterior}
	var maisAntiga time.Time

	for _, it := range r.Itens {
		a, err := normalizar(it, coletadoEm)
		if err != nil {
			lote.Descartes = append(lote.Descartes, Descarte{ID: it.Musica.ID, Motivo: err.Error()})
			continue
		}
		lote.Atividades = append(lote.Atividades, a)

		if a.Quando.After(lote.NovoCursor) {
			lote.NovoCursor = a.Quando
		}
		if maisAntiga.IsZero() || a.Quando.Before(maisAntiga) {
			maisAntiga = a.Quando
		}
	}

	if lacuna, ok := detectarLacuna(len(r.Itens), cursorAnterior, maisAntiga, coletadoEm); ok {
		lote.Atividades = append(lote.Atividades, lacuna)
	}
	return lote, nil
}

func normalizar(it item, coletadoEm time.Time) (atividade.Atividade, error) {
	if it.TocadaEm == nil {
		return atividade.Atividade{}, fmt.Errorf("item sem played_at")
	}
	if it.Musica.ID == "" {
		// Faixas locais e alguns episódios vêm sem id: sem id não há dedup confiável.
		return atividade.Atividade{}, fmt.Errorf("item sem id de faixa")
	}

	artistas := make([]string, 0, len(it.Musica.Artistas))
	for _, ar := range it.Musica.Artistas {
		artistas = append(artistas, ar.Nome)
	}
	dados, err := json.Marshal(map[string]any{
		"musica_id":  it.Musica.ID,
		"musica":     it.Musica.Nome,
		"artistas":   artistas,
		"album":      it.Musica.Album.Nome,
		"duracao_ms": it.Musica.DuracaoMs,
	})
	if err != nil {
		return atividade.Atividade{}, fmt.Errorf("serializar dados: %w", err)
	}

	quando := it.TocadaEm.UTC()
	a := atividade.Atividade{
		VersaoSchema: atividade.VersaoSchema,
		Fonte:        atividade.FonteSpotify,
		// A mesma música pode tocar várias vezes; o momento em que tocou torna o id único.
		IDExterno:  quando.Format(time.RFC3339Nano) + ":" + it.Musica.ID,
		Tipo:       atividade.MusicaOuvida,
		Quando:     quando,
		ColetadoEm: coletadoEm.UTC(),
		Dados:      dados,
	}
	if err := a.Validar(); err != nil {
		return atividade.Atividade{}, err
	}
	return a, nil
}

// detectarLacuna: se a resposta veio cheia (50 itens) e até a música mais antiga é
// mais nova que o cursor, houve músicas entre o cursor e ela que a API já descartou.
// Com menos de 50 itens a janela não encheu, então nada se perdeu.
// Limite conhecido: se tocaram exatamente 50 desde o cursor, nada se perdeu e mesmo
// assim a lacuna é registrada. A API não dá como distinguir; o falso positivo é o
// lado seguro do erro (avisar de uma lacuna que não houve, nunca esconder uma real).
func detectarLacuna(qtdItens int, cursorAnterior, maisAntiga, coletadoEm time.Time) (atividade.Atividade, bool) {
	if cursorAnterior.IsZero() || maisAntiga.IsZero() || qtdItens < limiteItens || !maisAntiga.After(cursorAnterior) {
		return atividade.Atividade{}, false
	}

	de, ate := cursorAnterior.UTC(), maisAntiga.UTC()
	dados, _ := json.Marshal(map[string]string{
		"de":  de.Format(time.RFC3339Nano),
		"ate": ate.Format(time.RFC3339Nano),
	})
	return atividade.Atividade{
		VersaoSchema: atividade.VersaoSchema,
		Fonte:        atividade.FonteSpotify,
		// Determinístico: o mesmo intervalo detectado de novo cai no dedup do event store.
		IDExterno:  "lacuna:" + de.Format(time.RFC3339Nano) + ":" + ate.Format(time.RFC3339Nano),
		Tipo:       atividade.LacunaDetectada,
		Quando:     ate,
		ColetadoEm: coletadoEm.UTC(),
		Dados:      dados,
	}, true
}
