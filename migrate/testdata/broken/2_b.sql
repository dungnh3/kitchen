-- +goose Up
CREATE TABLE b (id int);
SELECT broken syntax here;

-- +goose Down
DROP TABLE b;
