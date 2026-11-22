-- +goose Up
CREATE TABLE a (id int);

-- +goose Down
DROP TABLE a;
