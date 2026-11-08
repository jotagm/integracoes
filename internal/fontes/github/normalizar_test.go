package github

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
)

var coletadoEm = time.Date(2026, 11, 9, 22, 0, 0, 0, time.UTC)

func TestNormalizarLote_Fixture(t *testing.T) {
	corpo, err := os.ReadFile("testdata/eventos_publicos.json")
	if err != nil {
		t.Fatal(err)
	}

	atividades, descartes, err := NormalizarLote(corpo, coletadoEm)
	if err != nil {
		t.Fatalf("lote não deveria falhar: %v", err)
	}

	// 7 eventos: 4 viram atividade, 2 são tipos ignorados, 1 é descartado.
	if got := len(atividades); got != 4 {
		t.Fatalf("esperava 4 atividades, veio %d", got)
	}
	if len(descartes) != 1 || descartes[0].ID != "50007" {
		t.Fatalf("esperava descartar só o 50007 (sem created_at), veio %+v", descartes)
	}

	esperado := []struct {
		id   string
		tipo atividade.Tipo
	}{
		{"50001", atividade.CommitEnviado},
		{"50002", atividade.CommitEnviado},
		{"50003", atividade.RepositorioCriado},
		{"50005", atividade.EstrelaDada},
	}
	for i, e := range esperado {
		a := atividades[i]
		if a.IDExterno != e.id || a.Tipo != e.tipo {
			t.Errorf("posição %d: esperava %s/%s, veio %s/%s", i, e.id, e.tipo, a.IDExterno, a.Tipo)
		}
		if a.Fonte != atividade.FonteGitHub || !a.ColetadoEm.Equal(coletadoEm) {
			t.Errorf("posição %d: fonte ou coletado_em errados: %+v", i, a)
		}
		if err := a.Validar(); err != nil {
			t.Errorf("posição %d: atividade inválida: %v", i, err)
		}
	}
}

func TestNormalizar_Push(t *testing.T) {
	quando := time.Date(2026, 11, 9, 21, 14, 3, 0, time.UTC)

	casos := []struct {
		nome        string
		payload     string
		querBranch  string
		querCommits int
	}{
		{"size presente vence a lista", `{"ref":"refs/heads/main","size":5,"commits":[{}]}`, "main", 5},
		{"sem size conta a lista", `{"ref":"refs/heads/feat/x","commits":[{},{}]}`, "feat/x", 2},
		{"sem size e sem lista", `{"ref":"refs/heads/main"}`, "main", 0},
		{"ref que não é branch fica como veio", `{"ref":"refs/tags/v1","size":1}`, "refs/tags/v1", 1},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			e := evento{ID: "1", Tipo: "PushEvent", CriadoEm: &quando, Repo: repo{Nome: "jotagm/x"}, Payload: json.RawMessage(c.payload)}
			a, ok, err := normalizar(e, coletadoEm)
			if err != nil || !ok {
				t.Fatalf("esperava atividade, veio ok=%v err=%v", ok, err)
			}
			var dados struct {
				Branch  string `json:"branch"`
				Commits int    `json:"commits"`
			}
			if err := json.Unmarshal(a.Dados, &dados); err != nil {
				t.Fatal(err)
			}
			if dados.Branch != c.querBranch || dados.Commits != c.querCommits {
				t.Fatalf("esperava %s/%d, veio %s/%d", c.querBranch, c.querCommits, dados.Branch, dados.Commits)
			}
		})
	}
}

func TestNormalizar_TiposEFiltros(t *testing.T) {
	quando := time.Date(2026, 11, 9, 21, 0, 0, 0, time.UTC)

	casos := []struct {
		nome     string
		tipo     string
		payload  string
		querOK   bool
		querTipo atividade.Tipo
		querErr  bool
	}{
		{"repositório criado", "CreateEvent", `{"ref_type":"repository"}`, true, atividade.RepositorioCriado, false},
		{"branch criada é ignorada", "CreateEvent", `{"ref_type":"branch"}`, false, "", false},
		{"tag criada é ignorada", "CreateEvent", `{"ref_type":"tag"}`, false, "", false},
		{"estrela dada", "WatchEvent", `{"action":"started"}`, true, atividade.EstrelaDada, false},
		{"watch com outra ação é ignorado", "WatchEvent", `{"action":"other"}`, false, "", false},
		{"tipo desconhecido é ignorado", "IssuesEvent", `{}`, false, "", false},
		{"payload ilegível vira erro", "PushEvent", `[`, false, "", true},
		{"create ilegível vira erro", "CreateEvent", `[`, false, "", true},
		{"watch ilegível vira erro", "WatchEvent", `[`, false, "", true},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			e := evento{ID: "1", Tipo: c.tipo, CriadoEm: &quando, Repo: repo{Nome: "jotagm/x"}, Payload: json.RawMessage(c.payload)}
			a, ok, err := normalizar(e, coletadoEm)
			if (err != nil) != c.querErr {
				t.Fatalf("erro: esperava %v, veio %v", c.querErr, err)
			}
			if ok != c.querOK {
				t.Fatalf("ok: esperava %v, veio %v", c.querOK, ok)
			}
			if ok && a.Tipo != c.querTipo {
				t.Fatalf("tipo: esperava %s, veio %s", c.querTipo, a.Tipo)
			}
		})
	}
}

func TestNormalizar_SemID(t *testing.T) {
	quando := time.Now()
	_, _, err := normalizar(evento{Tipo: "PushEvent", CriadoEm: &quando, Payload: json.RawMessage(`{}`)}, coletadoEm)
	if err == nil {
		t.Fatal("evento sem id deveria dar erro")
	}
}

func TestNormalizarLote_CorpoIlegivel(t *testing.T) {
	if _, _, err := NormalizarLote([]byte(`{"message":"Bad credentials"}`), coletadoEm); err == nil {
		t.Fatal("um objeto no lugar do array deveria dar erro")
	}
}
