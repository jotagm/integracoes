// Package mensagem define o formato do evento no Kafka: o contrato entre o relay,
// que escreve, e o projetor, que lê. Mudar este formato é mudar o contrato.
package mensagem

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Evento é uma linha do event store já com o seq, que vira a versão no lado de leitura.
type Evento struct {
	Seq          int64           `json:"seq"`
	Fonte        string          `json:"fonte"`
	IDExterno    string          `json:"id_externo"`
	Tipo         string          `json:"tipo"`
	VersaoSchema int             `json:"versao_schema"`
	Quando       time.Time       `json:"quando"`
	ColetadoEm   time.Time       `json:"coletado_em"`
	Dados        json.RawMessage `json:"dados"`
}

// ErrInvalido é devolvido (embrulhado) quando uma mensagem não respeita o contrato.
var ErrInvalido = errors.New("mensagem inválida")

// Chave é a chave da mensagem no Kafka. Particionar por fonte garante a ordem
// dentro de cada fonte, que é o que o lado de leitura precisa.
func (e Evento) Chave() []byte { return []byte(e.Fonte) }

// Codificar serializa o evento para o valor da mensagem.
func (e Evento) Codificar() ([]byte, error) {
	if err := e.Validar(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// Decodificar lê o valor de uma mensagem e valida o contrato.
func Decodificar(valor []byte) (Evento, error) {
	var e Evento
	if err := json.Unmarshal(valor, &e); err != nil {
		return Evento{}, fmt.Errorf("%w: %w", ErrInvalido, err)
	}
	if err := e.Validar(); err != nil {
		return Evento{}, err
	}
	return e, nil
}

// Validar confere os campos que o projetor assume que existem.
func (e Evento) Validar() error {
	switch {
	case e.Seq <= 0:
		return fmt.Errorf("%w: seq ausente", ErrInvalido)
	case e.Fonte == "":
		return fmt.Errorf("%w: fonte ausente", ErrInvalido)
	case e.IDExterno == "":
		return fmt.Errorf("%w: id_externo ausente", ErrInvalido)
	case e.Tipo == "":
		return fmt.Errorf("%w: tipo ausente", ErrInvalido)
	case e.VersaoSchema <= 0:
		return fmt.Errorf("%w: versao_schema ausente", ErrInvalido)
	case e.Quando.IsZero():
		return fmt.Errorf("%w: quando ausente", ErrInvalido)
	case len(e.Dados) == 0 || !json.Valid(e.Dados):
		return fmt.Errorf("%w: dados ausentes ou JSON inválido", ErrInvalido)
	}
	return nil
}
