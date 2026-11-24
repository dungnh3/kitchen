-- The transactional outbox of github.com/dungnh3/kitchen/pubsub/outbox/pgx
-- (its Store.Schema): events are written here in the same transaction as
-- the data, and the worker's relay publishes them.

-- +goose Up
CREATE TABLE kitchen_outbox (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         text        NOT NULL,
    ordering_key  text        NOT NULL DEFAULT '',
    payload       bytea       NOT NULL,
    attributes    jsonb       NOT NULL DEFAULT '{}',
    attempts      integer     NOT NULL DEFAULT 0,
    available_at  timestamptz NOT NULL DEFAULT now(),
    claimed_until timestamptz,
    last_error    text,
    dead_at       timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    claim_token   bigint      NOT NULL DEFAULT 0
);
CREATE INDEX kitchen_outbox_key_idx ON kitchen_outbox (ordering_key, id) WHERE ordering_key <> '' AND dead_at IS NULL;
CREATE INDEX kitchen_outbox_free_idx ON kitchen_outbox (id) WHERE ordering_key = '' AND dead_at IS NULL;

-- +goose Down
DROP TABLE kitchen_outbox;
