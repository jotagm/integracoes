package github

import (
	"context"
	"fmt"
	"time"

	"github.com/jotagm/integracoes/internal/atividade"
	"github.com/jotagm/integracoes/internal/fonte"
)

// Fonte adapta o cliente e a normalização do GitHub ao contrato fonte.Fonte.
// O cursor do GitHub é só o ETag: o dedup por id do evento fica no event store.
type Fonte struct {
	cliente *Cliente
	usuario string
	agora   func() time.Time
}

// NovaFonte cria a fonte de eventos públicos de um usuário.
func NovaFonte(c *Cliente, usuario string) *Fonte {
	return &Fonte{cliente: c, usuario: usuario, agora: time.Now}
}

// Nome identifica a fonte no event store e no Kafka.
func (f *Fonte) Nome() atividade.Fonte { return atividade.FonteGitHub }

// Coletar faz um poll condicional e normaliza o que mudou.
func (f *Fonte) Coletar(ctx context.Context, cur fonte.Cursor) (fonte.Coleta, error) {
	res, err := f.cliente.EventosPublicos(ctx, f.usuario, cur.ETag)
	if err != nil {
		return fonte.Coleta{}, err
	}

	coleta := fonte.Coleta{
		NovoCursor:  fonte.Cursor{ETag: res.ETag},
		ProximoPoll: res.IntervaloPoll,
	}
	if res.NaoMudou {
		return coleta, nil
	}

	atividades, descartes, err := NormalizarLote(res.Corpo, f.agora())
	if err != nil {
		// 200 com corpo que não é a lista de eventos (página de erro de proxy, por exemplo):
		// tratamos como transitório e não avançamos o ETag.
		return fonte.Coleta{}, fmt.Errorf("%w: %w", fonte.ErrTransitorio, err)
	}
	coleta.Atividades = atividades
	coleta.Descartes = len(descartes)
	return coleta, nil
}
