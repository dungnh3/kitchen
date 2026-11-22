-- +goose Up
ALTER TABLE items ADD COLUMN price int;

-- +goose Down
ALTER TABLE items DROP COLUMN price;
