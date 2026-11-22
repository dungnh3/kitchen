-- +goose Up
ALTER TABLE items ADD COLUMN price INT;

-- +goose Down
ALTER TABLE items DROP COLUMN price;
