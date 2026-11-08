package spotify

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
)

var (
	coletadoEm = time.Date(2026, 11, 9, 22, 0, 0, 0, time.UTC)
	base       = time.Date(2026, 11, 9, 20, 0, 0, 0, time.UTC)
)

// respostaCom monta uma resposta do Spotify com n músicas, a cada 3 minutos a partir
// de inicio, da mais nova para a mais antiga, como a API devolve.
func respostaCom(n int, inicio time.Time) []byte {
	itens := make([]string, 0, n)
	for i := n - 1; i >= 0; i-- {
		tocada := inicio.Add(time.Duration(i) * 3 * time.Minute)
		itens = append(itens, fmt.Sprintf(
			`{"played_at":%q,"track":{"id":"faixa-%d","name":"Música %d","duration_ms":180000,"artists":[{"name":"Artista A"},{"name":"Artista B"}],"album":{"name":"Álbum"}}}`,
			tocada.Format(time.RFC3339Nano), i, i))
	}
	return []byte(`{"items":[` + strings.Join(itens, ",") + `]}`)
}

func TestNormalizarMusicas(t *testing.T) {
	lote, err := NormalizarLote(respostaCom(3, base), time.Time{}, coletadoEm)
	if err != nil {
		t.Fatal(err)
	}
	if len(lote.Atividades) != 3 || len(lote.Descartes) != 0 {
		t.Fatalf("esperava 3 atividades, veio %d (descartes %v)", len(lote.Atividades), lote.Descartes)
	}

	maisNova := lote.Atividades[0]
	querQuando := base.Add(6 * time.Minute)
	if maisNova.Tipo != atividade.MusicaOuvida || maisNova.Fonte != atividade.FonteSpotify || !maisNova.Quando.Equal(querQuando) {
		t.Fatalf("atividade errada: %+v", maisNova)
	}
	if maisNova.IDExterno != querQuando.Format(time.RFC3339Nano)+":faixa-2" {
		t.Fatalf("id_externo: %s", maisNova.IDExterno)
	}

	var dados struct {
		Artistas []string `json:"artistas"`
		Album    string   `json:"album"`
	}
	if err := json.Unmarshal(maisNova.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	if strings.Join(dados.Artistas, ",") != "Artista A,Artista B" || dados.Album != "Álbum" {
		t.Fatalf("dados: %+v", dados)
	}

	if !lote.NovoCursor.Equal(querQuando) {
		t.Fatalf("cursor deveria ser a mais nova (%s), veio %s", querQuando, lote.NovoCursor)
	}
}

func TestMesmaMusicaDuasVezesViraDoisEventos(t *testing.T) {
	corpo := []byte(`{"items":[
		{"played_at":"2026-11-09T20:03:00Z","track":{"id":"igual","name":"Repetida"}},
		{"played_at":"2026-11-09T20:00:00Z","track":{"id":"igual","name":"Repetida"}}
	]}`)
	lote, err := NormalizarLote(corpo, time.Time{}, coletadoEm)
	if err != nil {
		t.Fatal(err)
	}
	if len(lote.Atividades) != 2 || lote.Atividades[0].IDExterno == lote.Atividades[1].IDExterno {
		t.Fatalf("ouvir de novo é outro evento, ids precisam ser diferentes: %+v", lote.Atividades)
	}
}

func TestSemItensMantemOCursor(t *testing.T) {
	lote, err := NormalizarLote([]byte(`{"items":[]}`), base, coletadoEm)
	if err != nil {
		t.Fatal(err)
	}
	if len(lote.Atividades) != 0 || !lote.NovoCursor.Equal(base) {
		t.Fatalf("sem itens o cursor não pode andar nem voltar: %+v", lote)
	}
}

func TestItensQuebradosViramDescarte(t *testing.T) {
	corpo := []byte(`{"items":[
		{"played_at":"2026-11-09T20:03:00Z","track":{"id":"ok","name":"Boa"}},
		{"track":{"id":"sem-data","name":"Sem played_at"}},
		{"played_at":"2026-11-09T20:01:00Z","track":{"name":"Faixa local sem id"}}
	]}`)
	lote, err := NormalizarLote(corpo, time.Time{}, coletadoEm)
	if err != nil {
		t.Fatal(err)
	}
	if len(lote.Atividades) != 1 || len(lote.Descartes) != 2 {
		t.Fatalf("esperava 1 atividade e 2 descartes, veio %d e %d", len(lote.Atividades), len(lote.Descartes))
	}
}

func TestCorpoIlegivel(t *testing.T) {
	if _, err := NormalizarLote([]byte(`<html>`), time.Time{}, coletadoEm); err == nil {
		t.Fatal("corpo que não é JSON deveria dar erro")
	}
}

func TestLacuna(t *testing.T) {
	// Janela cheia: 50 músicas começando em base. A mais antiga tocou em base.
	cheia := respostaCom(50, base)

	casos := []struct {
		nome       string
		corpo      []byte
		cursor     time.Time
		querLacuna bool
	}{
		{"janela cheia e a mais antiga depois do cursor", cheia, base.Add(-time.Hour), true},
		{"janela cheia mas o cursor está dentro dela", cheia, base.Add(30 * time.Minute), false},
		{"cursor exatamente na mais antiga", cheia, base, false},
		{"janela não encheu", respostaCom(49, base), base.Add(-time.Hour), false},
		{"primeiro poll nunca tem lacuna", cheia, time.Time{}, false},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			lote, err := NormalizarLote(c.corpo, c.cursor, coletadoEm)
			if err != nil {
				t.Fatal(err)
			}
			var lacunas []atividade.Atividade
			for _, a := range lote.Atividades {
				if a.Tipo == atividade.LacunaDetectada {
					lacunas = append(lacunas, a)
				}
			}
			if (len(lacunas) == 1) != c.querLacuna || len(lacunas) > 1 {
				t.Fatalf("lacunas: esperava %v, veio %d", c.querLacuna, len(lacunas))
			}
			if !c.querLacuna {
				return
			}

			l := lacunas[0]
			if err := l.Validar(); err != nil {
				t.Fatal(err)
			}
			var dados struct{ De, Ate string }
			if err := json.Unmarshal(l.Dados, &dados); err != nil {
				t.Fatal(err)
			}
			if dados.De != c.cursor.Format(time.RFC3339Nano) || dados.Ate != base.Format(time.RFC3339Nano) {
				t.Fatalf("intervalo da lacuna: %+v", dados)
			}
		})
	}
}

func TestLacunaTemIDDeterministico(t *testing.T) {
	cursor := base.Add(-time.Hour)
	a, _ := NormalizarLote(respostaCom(50, base), cursor, coletadoEm)
	b, _ := NormalizarLote(respostaCom(50, base), cursor, coletadoEm.Add(time.Hour))
	idDe := func(l Lote) string {
		for _, x := range l.Atividades {
			if x.Tipo == atividade.LacunaDetectada {
				return x.IDExterno
			}
		}
		return ""
	}
	if idDe(a) == "" || idDe(a) != idDe(b) {
		t.Fatalf("a mesma lacuna detectada duas vezes precisa do mesmo id: %q x %q", idDe(a), idDe(b))
	}
}
