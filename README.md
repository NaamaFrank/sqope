
# sqope AI

Lightweight toolkit for indexing documents and answering text + table analytics queries.

## Overview

This repository contains two main components:

- **Indexer**: ingests files (PDFs, etc.), normalizes rows, and stores schema embeddings.
- **API service**: FastAPI server that answers free-text and analytical questions by combining vector similarity, a small LLM planner, and SQL executed against JSONB rows.

## Prerequisites

- **Docker & Docker Compose only** — no local Python, database, or model setup required
- PowerShell (Windows) or bash (macOS / Linux) to run the helper scripts (optional)

## Architecture

This project uses a **hybrid Docker approach**:

- **Docker Compose** (`docker compose up`): Orchestrates the API and database services as a continuous environment
- **`docker run` standalone**: Allows on-demand indexing without Docker Compose

This design means:
- Start the API + DB once: `docker compose up -d db ollama api`
- Run indexing independently as needed: `./scripts/run-indexer.ps1 -FilePath "..."`
- Both connect to the same database over the `sqope_default` network

No local installation needed — everything runs in containers.

## Quick Start

1. **Configure environment**: 
   - Copy `.env.example` to `.env` at the repo root
   - You can use the default values as-is, or customize them (e.g., database password)
   
   ```powershell
   cp .env.example .env
   # Edit .env if you want to change defaults (optional)
   ```

2. **Start all services** using Docker Compose:

   ```powershell
   docker compose up --build -d
   ```

   This will start the database, Ollama, API, and indexer in one command. The `api` service depends on `db` and `ollama` and uses healthchecks, so Compose will wait for those services to be healthy before starting the API.

### Alternative Docker Compose Commands

- Start only infra (DB and Ollama):
  ```powershell
  docker compose up -d db ollama
  ```

- Start only the API (automatically starts db and ollama):
  ```powershell
  docker compose up api --build -d
  ```

- Check logs:
  ```powershell
  docker compose logs -f api
  ```

- Stop all services:
  ```powershell
  docker compose down
  ```

## Indexing Documents

We provide two wrapper scripts that simplify running the indexer container and mounting files. **Run these scripts from the repository root.**

### Available Scripts

- `scripts/run-indexer.ps1` — PowerShell wrapper (Windows, PowerShell Core on macOS/Linux)
- `scripts/run-indexer.sh` — Bash wrapper (macOS / Linux)

### Usage Examples

**PowerShell:**
```powershell
.\scripts\run-indexer.ps1 -FilePath "C:\full\path\to\your.pdf"
```

**Bash:**
```bash
./scripts/run-indexer.sh -f /full/path/to/file.pdf
```

### Manual Docker Run (Advanced)

If you prefer to run the indexer container manually:

```bash
# Build image (if needed)
docker build -f docker/Dockerfile.indexer -t sqope-indexer .

# Run indexer
docker run --rm --network sqope_default --env-file .env \
  -v "/full/path/to/file.pdf:/data/file.pdf:ro" \
  sqope-indexer --path /data/file.pdf
```

## Using the API

The API exposes a `/query` endpoint (POST) that accepts JSON: `{"question": "..."}`.

### Examples

**Bash / macOS / Linux:**
```bash
curl -sS -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the q4 highlights for Acme Corp?"}'
```

**PowerShell:**
```powershell
$body = @{ question = "What are the q4 highlights for Acme Corp?" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8000/query -Method Post -Body $body -ContentType 'application/json'
```
