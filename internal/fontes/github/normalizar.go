// Package github traduz os eventos públicos do GitHub para o vocabulário do projeto.
// Este arquivo não faz IO: recebe o corpo da resposta e devolve atividades.
// O cliente HTTP (ETag, X-Poll-Interval, 429) entra no dia 2.
package github

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
)

// evento é o recorte da resposta de GET /users/{usuario}/events/public que nos interessa.
type evento struct {
	ID       string          `json:"id"`
	Tipo     string          `json:"type"`
	CriadoEm *time.Time      `json:"created_at"`
	Repo     repo            `json:"repo"`
	Payload  json.RawMessage `json:"payload"`
}

type repo struct {
	Nome string `json:"name"`
}

// pushPayload aceita os dois formatos do PushEvent. Desde out/2025 a Events API
// não manda mais size nem commits, só ref e os SHAs before/head; payloads antigos
// (e o formato de webhook) ainda trazem a contagem.
type pushPayload struct {
	Ref     string            `json:"ref"`
	Head    string            `json:"head"`
	Antes   string            `json:"before"`
	Size    *int              `json:"size"`
	Commits []json.RawMessage `json:"commits"`
}

type createPayload struct {
	RefType string `json:"ref_type"`
}

type watchPayload struct {
	Action string `json:"action"`
}

// Descarte registra um evento que não virou atividade e por quê.
// Tipos que o projeto não acompanha não entram aqui: são só ignorados.
type Descarte struct {
	ID     string
	Motivo string
}

// NormalizarLote lê o corpo de uma resposta da API (um array JSON de eventos).
// Devolve erro só se o corpo inteiro for ilegível; um evento quebrado vira Descarte
// para não travar o resto do lote.
func NormalizarLote(corpo []byte, coletadoEm time.Time) ([]atividade.Atividade, []Descarte, error) {
	var eventos []evento
	if err := json.Unmarshal(corpo, &eventos); err != nil {
		return nil, nil, fmt.Errorf("github: corpo ilegível: %w", err)
	}

	var (
		atividades []atividade.Atividade
		descartes  []Descarte
	)
	for _, e := range eventos {
		a, ok, err := normalizar(e, coletadoEm)
		if err != nil {
			descartes = append(descartes, Descarte{ID: e.ID, Motivo: err.Error()})
			continue
		}
		if ok {
			atividades = append(atividades, a)
		}
	}
	return atividades, descartes, nil
}

// normalizar devolve ok=false para tipos que o projeto não acompanha.
func normalizar(e evento, coletadoEm time.Time) (atividade.Atividade, bool, error) {
	if e.ID == "" {
		return atividade.Atividade{}, false, fmt.Errorf("evento sem id")
	}
	if e.CriadoEm == nil {
		return atividade.Atividade{}, false, fmt.Errorf("evento sem created_at")
	}

	var (
		tipo  atividade.Tipo
		dados any
	)

	switch e.Tipo {
	case "PushEvent":
		var p pushPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return atividade.Atividade{}, false, fmt.Errorf("payload de push ilegível: %w", err)
		}
		tipo = atividade.CommitEnviado
		d := map[string]any{
			"repositorio": e.Repo.Nome,
			"branch":      strings.TrimPrefix(p.Ref, "refs/heads/"),
		}
		// Sem contagem no payload, "commits" fica de fora: gravar 0 seria afirmar
		// algo falso num event store que não aceita correção.
		switch {
		case p.Size != nil:
			d["commits"] = *p.Size
		case len(p.Commits) > 0:
			d["commits"] = len(p.Commits)
		}
		if p.Head != "" {
			d["head"] = p.Head
		}
		if p.Antes != "" {
			d["antes"] = p.Antes
		}
		dados = d

	case "CreateEvent":
		var p createPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return atividade.Atividade{}, false, fmt.Errorf("payload de create ilegível: %w", err)
		}
		if p.RefType != "repository" {
			return atividade.Atividade{}, false, nil // branch ou tag: não acompanhamos
		}
		tipo = atividade.RepositorioCriado
		dados = map[string]any{"repositorio": e.Repo.Nome}

	case "WatchEvent":
		var p watchPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return atividade.Atividade{}, false, fmt.Errorf("payload de watch ilegível: %w", err)
		}
		if p.Action != "started" {
			return atividade.Atividade{}, false, nil
		}
		tipo = atividade.EstrelaDada
		dados = map[string]any{"repositorio": e.Repo.Nome}

	default:
		return atividade.Atividade{}, false, nil
	}

	bruto, err := json.Marshal(dados)
	if err != nil {
		return atividade.Atividade{}, false, fmt.Errorf("serializar dados: %w", err)
	}

	a := atividade.Atividade{
		VersaoSchema: atividade.VersaoSchema,
		Fonte:        atividade.FonteGitHub,
		IDExterno:    e.ID,
		Tipo:         tipo,
		Quando:       e.CriadoEm.UTC(),
		ColetadoEm:   coletadoEm.UTC(),
		Dados:        bruto,
	}
	if err := a.Validar(); err != nil {
		return atividade.Atividade{}, false, err
	}
	return a, true, nil
}
