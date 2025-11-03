# sqope

Small README with usage for the indexer CLI run helper (Windows PowerShell).

## Running the indexer from PowerShell (user-friendly)

Helper scripts are provided so users can run a single-file indexing command without worrying about Docker mount paths or container internals.

Available wrappers:

- `scripts/run-indexer.ps1` — PowerShell wrapper (works on Windows, PowerShell Core on macOS/Linux)
- `scripts/run-indexer.sh` — Bash wrapper for macOS / Linux

Usage (run from the repository root):


Mount a single PDF and index it (PowerShell):

```powershell
.\scripts\run-indexer.ps1 -FilePath "C:\full\path\to\your.pdf" -DocId your_doc_id
```

Mount a single PDF and index it (bash / macOS / Linux):

```bash
# build the image if needed
docker build -f docker/Dockerfile.indexer -t sqope-indexer .

# run and mount a local file into the container
docker run --rm --env-file .env -v "/full/path/to/your.pdf:/data/your.pdf:ro" --name sqope-indexer sqope-indexer file --path "/data/your.pdf" --doc-id your_doc_id
```

If you prefer to mount the containing folder (so multiple files are available) use the wrapper's `-MountParent` / `--mount-parent` option to mount the file's parent directory, or `-MountFolder <path>` / `--mount-folder <path>` to mount any folder. For the manual container run, mount the folder instead of a single file:

```bash
docker run --rm --env-file .env -v "/full/path/to/folder:/host_files:ro" --name sqope-indexer sqope-indexer file --path "/host_files/your.pdf" --doc-id your_doc_id
```

If the Docker image `sqope-indexer` is not present locally, build it first (the helpers can build for you using `-Build` / `--build`):

PowerShell:
```powershell
.\scripts\run-indexer.ps1 -FilePath "C:\path\to\file.pdf" -DocId myid -Build
```

Bash / macOS / Linux:
```bash
./scripts/run-indexer.sh --file /full/path/to/file.pdf --id myid --build
```

Notes
- The PowerShell helper mounts the file (or folder) into the container under `/data` (or `/host_files`) and invokes the indexer subcommand: `python -m indexer file --path <container-path> --doc-id <docid>`.
- For non-PowerShell users on macOS/Linux use the manual `docker run` examples above. Do not hardcode host usernames or OS-specific paths in scripts—use the provided CLI or explicit host paths when running the container.
- If you use `docker compose` to run `db` and `ollama`, the script will attempt to use the `sqope_default` network. Start those services first:

```powershell
docker compose up -d db ollama
```

- If you prefer to run the indexer container manually, here's the equivalent manual flow:

```powershell
# build the image
docker build -f docker/Dockerfile.indexer -t sqope-indexer .

# run with a single-file mount (adjust network as needed)
docker run --rm --network sqope_default --env-file .env -v "C:\Users\Naama\Downloads\sqope_ai_home_assignment.pdf:/data/sqope_ai_home_assignment.pdf:ro" --name sqope-indexer sqope-indexer file --path "/data/sqope_ai_home_assignment.pdf" --doc-id sqope_ai_home_assignment
```

Troubleshooting
- If Typer reports "File not found", confirm the host file was mounted into the container and you passed the container path (the helper script does this for you).
- Inspect logs with `docker logs -f <container-name>`.

Questions or custom needs? Open an issue or ping the maintainer.