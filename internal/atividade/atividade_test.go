package atividade

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func valida() Atividade {
	t := time.Date(2026, 11, 9, 21, 14, 3, 0, time.UTC)
	return Atividade{
		VersaoSchema: VersaoSchema,
		Fonte:        FonteGitHub,
		IDExterno:    "123",
		Tipo:         CommitEnviado,
		Quando:       t,
		ColetadoEm:   t.Add(5 * time.Minute),
		Dados:        json.RawMessage(`{"repositorio":"jotagm/integracoes"}`),
	}
}

func TestValidar(t *testing.T) {
	casos := []struct {
		nome    string
		mudar   func(*Atividade)
		querErr bool
	}{
		{"completa", func(*Atividade) {}, false},
		{"sem versao", func(a *Atividade) { a.VersaoSchema = 0 }, true},
		{"sem fonte", func(a *Atividade) { a.Fonte = "" }, true},
		{"sem id externo", func(a *Atividade) { a.IDExterno = "" }, true},
		{"sem tipo", func(a *Atividade) { a.Tipo = "" }, true},
		{"sem quando", func(a *Atividade) { a.Quando = time.Time{} }, true},
		{"sem coletado_em", func(a *Atividade) { a.ColetadoEm = time.Time{} }, true},
		{"dados vazios", func(a *Atividade) { a.Dados = nil }, true},
		{"dados com JSON inválido", func(a *Atividade) { a.Dados = json.RawMessage(`{`) }, true},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			a := valida()
			c.mudar(&a)
			err := a.Validar()
			if c.querErr {
				if !errors.Is(err, ErrInvalida) {
					t.Fatalf("esperava ErrInvalida, veio %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("não esperava erro, veio %v", err)
			}
		})
	}
}

func TestJSONIdaEVolta(t *testing.T) {
	a := valida()
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var volta Atividade
	if err := json.Unmarshal(b, &volta); err != nil {
		t.Fatal(err)
	}
	if volta.IDExterno != a.IDExterno || !volta.Quando.Equal(a.Quando) || string(volta.Dados) != string(a.Dados) {
		t.Fatalf("ida e volta mudou o evento: %+v", volta)
	}
}
