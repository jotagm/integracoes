package eventstore

import (
	"context"
	"fmt"

	"github.com/jotagm/integracoes/internal/mensagem"
)

// FOR UPDATE OF o trava as linhas da outbox até o fim da transação. Um segundo
// relay que rodar ao mesmo tempo fica esperando aqui e, quando a primeira transação
// termina, o WHERE é reavaliado: o que já foi marcado sai do resultado. Isso
// mantém a ordem por seq e evita dois relays publicando o mesmo lote.
const sqlTravarPendentes = `
SELECT e.seq, e.fonte, e.id_externo, e.tipo, e.versao_schema, e.quando, e.coletado_em, e.dados
FROM outbox o
JOIN evento e ON e.seq = o.evento_seq
WHERE o.publicado_em IS NULL
ORDER BY o.evento_seq
LIMIT $1
FOR UPDATE OF o`

const sqlMarcarPublicados = `UPDATE outbox SET publicado_em = now() WHERE evento_seq = ANY($1)`

// ProcessarLote implementa relay.Outbox. A transação fica aberta durante o publicar:
// com lotes de 100 e um broker no mesmo lab, isso dura milissegundos.
func (s *Store) ProcessarLote(ctx context.Context, limite int, publicar func(context.Context, []mensagem.Evento) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("eventstore: abrir transação da outbox: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, sqlTravarPendentes, limite)
	if err != nil {
		return 0, fmt.Errorf("eventstore: travar pendentes: %w", err)
	}
	var lote []mensagem.Evento
	for rows.Next() {
		var e mensagem.Evento
		var dados []byte
		if err := rows.Scan(&e.Seq, &e.Fonte, &e.IDExterno, &e.Tipo, &e.VersaoSchema, &e.Quando, &e.ColetadoEm, &dados); err != nil {
			rows.Close()
			return 0, fmt.Errorf("eventstore: ler pendente: %w", err)
		}
		e.Dados = dados
		e.Quando = e.Quando.UTC()
		e.ColetadoEm = e.ColetadoEm.UTC()
		lote = append(lote, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("eventstore: percorrer pendentes: %w", err)
	}
	if len(lote) == 0 {
		return 0, nil
	}

	if err := publicar(ctx, lote); err != nil {
		return 0, err // rollback: nada marcado, as linhas destravam
	}

	seqs := make([]int64, len(lote))
	for i, e := range lote {
		seqs[i] = e.Seq
	}
	if _, err := tx.Exec(ctx, sqlMarcarPublicados, seqs); err != nil {
		return 0, fmt.Errorf("eventstore: marcar publicados: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		// O lote já está no Kafka. Sem o commit ele volta como pendente e sai de novo:
		// é o caso de at-least-once que o projetor resolve pelo seq.
		return 0, fmt.Errorf("eventstore: commit da outbox: %w", err)
	}
	return len(lote), nil
}
