package mensagem

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func valido() Evento {
	t := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	return Evento{
		Seq: 42, Fonte: "github", IDExterno: "50001", Tipo: "CommitEnviado",
		VersaoSchema: 1, Quando: t, ColetadoEm: t.Add(time.Minute),
		Dados: json.RawMessage(`{"repositorio":"jotagm/integracoes","commits":3}`),
	}
}

func TestIdaEVolta(t *testing.T) {
	e := valido()
	b, err := e.Codificar()
	if err != nil {
		t.Fatal(err)
	}
	volta, err := Decodificar(b)
	if err != nil {
		t.Fatal(err)
	}
	if volta.Seq != e.Seq || volta.IDExterno != e.IDExterno || !volta.Quando.Equal(e.Quando) {
		t.Fatalf("ida e volta mudou o evento: %+v", volta)
	}
	// Dados não pode ser reinterpretado: o projetor recebe exatamente o JSON gravado.
	if string(volta.Dados) != string(e.Dados) {
		t.Fatalf("dados mudaram: %s", volta.Dados)
	}
}

func TestChaveEAFonte(t *testing.T) {
	if string(valido().Chave()) != "github" {
		t.Fatal("a chave do Kafka tem que ser a fonte, senão a ordem por fonte se perde")
	}
}

func TestValidar(t *testing.T) {
	casos := []struct {
		nome  string
		mudar func(*Evento)
	}{
		{"sem seq", func(e *Evento) { e.Seq = 0 }},
		{"seq negativo", func(e *Evento) { e.Seq = -1 }},
		{"sem fonte", func(e *Evento) { e.Fonte = "" }},
		{"sem id externo", func(e *Evento) { e.IDExterno = "" }},
		{"sem tipo", func(e *Evento) { e.Tipo = "" }},
		{"sem versão", func(e *Evento) { e.VersaoSchema = 0 }},
		{"sem quando", func(e *Evento) { e.Quando = time.Time{} }},
		{"sem dados", func(e *Evento) { e.Dados = nil }},
		{"dados quebrados", func(e *Evento) { e.Dados = json.RawMessage(`{`) }},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			e := valido()
			c.mudar(&e)
			if _, err := e.Codificar(); !errors.Is(err, ErrInvalido) {
				t.Fatalf("Codificar deveria recusar, veio %v", err)
			}
		})
	}
}

func TestDecodificarRecusaLixo(t *testing.T) {
	for _, valor := range []string{`não é json`, `{}`, `{"seq":1}`} {
		if _, err := Decodificar([]byte(valor)); !errors.Is(err, ErrInvalido) {
			t.Fatalf("%q deveria ser recusado, veio %v", valor, err)
		}
	}
}
