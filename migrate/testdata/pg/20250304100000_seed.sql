-- +goose Up
INSERT INTO items (name) VALUES ('seed');

-- +goose Down
DELETE FROM items WHERE name = 'seed';
