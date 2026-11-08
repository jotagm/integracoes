package github

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// Garante em tempo de compilação que *Fonte implementa o contrato.
var _ fonte.Fonte = (*Fonte)(nil)

func TestFonteColetaENormaliza(t *testing.T) {
	fixture, err := os.ReadFile("testdata/eventos_publicos.json")
	if err != nil {
		t.Fatal(err)
	}
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v2"`)
		w.Header().Set("X-Poll-Interval", "90")
		_, _ = w.Write(fixture)
	})
	f := NovaFonte(c, "jotagm")
	f.agora = func() time.Time { return agoraFixo }

	coleta, err := f.Coletar(context.Background(), fonte.Cursor{ETag: `"v1"`})
	if err != nil {
		t.Fatal(err)
	}
	if f.Nome() != atividade.FonteGitHub {
		t.Fatalf("nome: %s", f.Nome())
	}
	if len(coleta.Atividades) != 4 || coleta.Descartes != 1 {
		t.Fatalf("esperava 4 atividades e 1 descarte, veio %d e %d", len(coleta.Atividades), coleta.Descartes)
	}
	if coleta.NovoCursor.ETag != `"v2"` || coleta.ProximoPoll != 90*time.Second {
		t.Fatalf("cursor/intervalo: %+v", coleta)
	}
	if !coleta.Atividades[0].ColetadoEm.Equal(agoraFixo) {
		t.Fatalf("coletado_em deveria vir do relógio da fonte: %s", coleta.Atividades[0].ColetadoEm)
	}
}

func TestFonteNaoMudouDevolveColetaVazia(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	})
	coleta, err := NovaFonte(c, "jotagm").Coletar(context.Background(), fonte.Cursor{ETag: `"v1"`})
	if err != nil {
		t.Fatal(err)
	}
	if len(coleta.Atividades) != 0 || coleta.NovoCursor.ETag != `"v1"` {
		t.Fatalf("304 não traz atividades e mantém o ETag: %+v", coleta)
	}
}

func TestFonteCorpoIlegivelETransitorio(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>proxy</html>`))
	})
	_, err := NovaFonte(c, "jotagm").Coletar(context.Background(), fonte.Cursor{})
	if !errors.Is(err, fonte.ErrTransitorio) {
		t.Fatalf("esperava transitório, veio %v", err)
	}
}

func TestFonteRepassaErroDoCliente(t *testing.T) {
	c := servidor(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := NovaFonte(c, "jotagm").Coletar(context.Background(), fonte.Cursor{})
	if !errors.Is(err, fonte.ErrPermanente) {
		t.Fatalf("404 deveria chegar ao ingestor como permanente, veio %v", err)
	}
}
