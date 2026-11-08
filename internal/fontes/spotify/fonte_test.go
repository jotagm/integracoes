package spotify

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

var _ fonte.Fonte = (*Fonte)(nil)

func TestFonteSpotifyAvancaOCursor(t *testing.T) {
	cursor := base.Add(-time.Minute)
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "" {
			t.Error("com cursor, a fonte deveria mandar after")
		}
		_, _ = w.Write(respostaCom(3, base))
	})
	f := NovaFonte(c, 0)
	f.agora = func() time.Time { return coletadoEm }

	coleta, err := f.Coletar(context.Background(), fonte.Cursor{Valor: cursor.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	if f.Nome() != atividade.FonteSpotify || coleta.ProximoPoll != IntervaloPadrao {
		t.Fatalf("nome/intervalo: %s %s", f.Nome(), coleta.ProximoPoll)
	}
	if len(coleta.Atividades) != 3 {
		t.Fatalf("esperava 3 atividades, veio %d", len(coleta.Atividades))
	}
	quer := base.Add(6 * time.Minute).Format(time.RFC3339Nano)
	if coleta.NovoCursor.Valor != quer {
		t.Fatalf("cursor: esperava %s, veio %s", quer, coleta.NovoCursor.Valor)
	}
}

func TestFonteSpotifySemNovidadeMantemOCursor(t *testing.T) {
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[]}`))
	})
	anterior := fonte.Cursor{Valor: base.Format(time.RFC3339Nano)}
	coleta, err := NovaFonte(c, time.Minute).Coletar(context.Background(), anterior)
	if err != nil {
		t.Fatal(err)
	}
	if coleta.NovoCursor != anterior || coleta.ProximoPoll != time.Minute {
		t.Fatalf("sem novidade, cursor igual: %+v", coleta)
	}
}

func TestFonteSpotifyPrimeiroPollSemCursor(t *testing.T) {
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("after") {
			t.Error("primeiro poll não manda after")
		}
		_, _ = w.Write([]byte(`{"items":[]}`))
	})
	coleta, err := NovaFonte(c, 0).Coletar(context.Background(), fonte.Cursor{})
	if err != nil || coleta.NovoCursor.Valor != "" {
		t.Fatalf("coleta=%+v err=%v", coleta, err)
	}
}

func TestFonteSpotifyCursorCorrompidoEPermanente(t *testing.T) {
	c := servidorAPI(t, &tokenFalso{}, func(w http.ResponseWriter, r *http.Request) {
		t.Error("não deveria chamar a API com cursor corrompido")
	})
	_, err := NovaFonte(c, 0).Coletar(context.Background(), fonte.Cursor{Valor: "ontem"})
	if !errors.Is(err, fonte.ErrPermanente) {
		t.Fatalf("esperava permanente, veio %v", err)
	}
}
