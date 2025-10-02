set dotenv-load := true

# Default task: show all recipes
default: help

help:
    @just --list

# Start Postgres + pgvector
db-up:
    docker compose up -d db

# Stop and remove containers
db-down:
    docker compose down

# Tail database logs
db-logs:
    docker compose logs -f db

# Open psql inside the running container
psql:
    docker compose exec db psql -U "${POSTGRES_USER:-rag}" -d "${POSTGRES_DB:-rag}"

# Run SQLx migrations via CLI (requires `cargo install sqlx-cli`)
migrate:
    DATABASE_URL="${DATABASE_URL}" sqlx migrate run

# Show migration status
migrate-info:
    DATABASE_URL="${DATABASE_URL}" sqlx migrate info

# Revert the last migration
migrate-revert:
    DATABASE_URL="${DATABASE_URL}" sqlx migrate revert

# Reset the DB volume and bring it back up (will rerun docker init.sql)
db-reset:
    docker compose down -v
    docker compose up -d db
