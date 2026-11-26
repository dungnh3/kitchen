-- name: GetItem :one
SELECT id, title, create_time FROM items WHERE id = $1;

-- name: ListItems :many
SELECT id, title, create_time FROM items
WHERE id > sqlc.arg(after)::uuid
ORDER BY id
LIMIT sqlc.arg(page_limit);

-- name: CreateItem :one
INSERT INTO items (title) VALUES ($1)
RETURNING id, title, create_time;

-- name: DeleteItem :execrows
DELETE FROM items WHERE id = $1;
