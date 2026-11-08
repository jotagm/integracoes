-- +goose Up

-- Fonte da verdade: só INSERT. seq é a ordem de gravação e vira a versão no lado de leitura.
CREATE TABLE evento (
    seq           bigserial   PRIMARY KEY,
    fonte         text        NOT NULL,
    id_externo    text        NOT NULL,
    tipo          text        NOT NULL,
    versao_schema int         NOT NULL,
    quando        timestamptz NOT NULL,
    coletado_em   timestamptz NOT NULL,
    dados         jsonb       NOT NULL,
    gravado_em    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT evento_fonte_id_externo_unico UNIQUE (fonte, id_externo)
);

-- Onde cada fonte parou. Gravado na mesma transação dos eventos.
CREATE TABLE cursor_fonte (
    fonte         text        PRIMARY KEY,
    cursor        text        NOT NULL DEFAULT '',
    etag          text        NOT NULL DEFAULT '',
    atualizado_em timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox: o relay publica no Kafka o que ainda não tem publicado_em.
CREATE TABLE outbox (
    evento_seq   bigint      PRIMARY KEY REFERENCES evento (seq),
    publicado_em timestamptz
);

CREATE INDEX outbox_pendente ON outbox (evento_seq) WHERE publicado_em IS NULL;

-- Append-only garantido pelo banco, não só pela disciplina do código.
-- +goose StatementBegin
CREATE FUNCTION evento_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'evento é append-only: % não é permitido', TG_OP;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER evento_sem_update_delete
    BEFORE UPDATE OR DELETE ON evento
    FOR EACH ROW EXECUTE FUNCTION evento_append_only();

CREATE TRIGGER evento_sem_truncate
    BEFORE TRUNCATE ON evento
    FOR EACH STATEMENT EXECUTE FUNCTION evento_append_only();

-- +goose Down
DROP TABLE outbox;
DROP TABLE cursor_fonte;
DROP TABLE evento;
DROP FUNCTION evento_append_only();
