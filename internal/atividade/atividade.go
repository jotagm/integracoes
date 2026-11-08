// Package atividade define o evento normalizado que todas as fontes produzem.
// Não faz IO: é só o modelo e as regras de validação.
package atividade

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// VersaoSchema é a versão atual do formato do evento. Mude quando o formato
// de Dados mudar de um jeito que um projetor antigo não saiba ler.
const VersaoSchema = 1

// Fonte identifica de onde a atividade veio. Também é a chave de partição no Kafka.
type Fonte string

const (
	FonteSpotify Fonte = "spotify"
	FonteGitHub  Fonte = "github"
)

// Tipo é o que aconteceu, já traduzido para o vocabulário do projeto.
type Tipo string

const (
	MusicaOuvida      Tipo = "MusicaOuvida"
	CommitEnviado     Tipo = "CommitEnviado"
	RepositorioCriado Tipo = "RepositorioCriado"
	EstrelaDada       Tipo = "EstrelaDada"
	LacunaDetectada   Tipo = "LacunaDetectada"
)

// Atividade é o evento normalizado. (Fonte, IDExterno) identifica o evento
// de forma única e é o que garante o dedup no event store.
type Atividade struct {
	VersaoSchema int             `json:"versao_schema"`
	Fonte        Fonte           `json:"fonte"`
	IDExterno    string          `json:"id_externo"`
	Tipo         Tipo            `json:"tipo"`
	Quando       time.Time       `json:"quando"`
	ColetadoEm   time.Time       `json:"coletado_em"`
	Dados        json.RawMessage `json:"dados"`
}

// ErrInvalida é devolvido (embrulhado) por Validar quando falta algo obrigatório.
var ErrInvalida = errors.New("atividade inválida")

// Validar confere os campos que o resto do sistema assume que existem.
func (a Atividade) Validar() error {
	switch {
	case a.VersaoSchema <= 0:
		return fmt.Errorf("%w: versao_schema ausente", ErrInvalida)
	case a.Fonte == "":
		return fmt.Errorf("%w: fonte ausente", ErrInvalida)
	case a.IDExterno == "":
		return fmt.Errorf("%w: id_externo ausente", ErrInvalida)
	case a.Tipo == "":
		return fmt.Errorf("%w: tipo ausente", ErrInvalida)
	case a.Quando.IsZero():
		return fmt.Errorf("%w: quando ausente", ErrInvalida)
	case a.ColetadoEm.IsZero():
		return fmt.Errorf("%w: coletado_em ausente", ErrInvalida)
	case len(a.Dados) == 0 || !json.Valid(a.Dados):
		return fmt.Errorf("%w: dados ausentes ou JSON inválido", ErrInvalida)
	}
	return nil
}
