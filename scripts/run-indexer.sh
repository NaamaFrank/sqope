#!/usr/bin/env bash
# Simple wrapper to run the indexer container from macOS/Linux
# Usage:
#   ./scripts/run-indexer.sh -f /full/path/to/file.pdf

set -euo pipefail
script_dir=$(dirname "$(readlink -f "$0")")
repo_root=$(readlink -f "$script_dir/..")

IMAGE_NAME="sqope-indexer"
NETWORK="sqope_default"
ENV_FILE=".env"
BUILD=false
MOUNT_PARENT=false
MOUNT_FOLDER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      FILEPATH="$2"; shift 2;;
    --mount-parent)
      MOUNT_PARENT=true; shift;;
    --mount-folder)
      MOUNT_FOLDER="$2"; shift 2;;
    --build)
      BUILD=true; shift;;
    --image)
      IMAGE_NAME="$2"; shift 2;;
    --network)
      NETWORK="$2"; shift 2;;
    --env-file)
      ENV_FILE="$2"; shift 2;;
    -*|--*)
      echo "Unknown option $1"; exit 1;;
    *)
      break;;
  esac
done

if [[ -z "${FILEPATH-}" ]]; then
  echo "Usage: $0 -f /full/path/to/file.pdf [--mount-parent|--mount-folder /path] [--build]"
  exit 2
fi

if [[ ! -f "$FILEPATH" ]]; then
  echo "File not found: $FILEPATH"; exit 2
fi

file_name=$(basename "$FILEPATH")
container_file_path="/data/$file_name"
volume_args=()

if [[ -n "$MOUNT_FOLDER" ]]; then
  if [[ ! -d "$MOUNT_FOLDER" ]]; then echo "Mount folder not found: $MOUNT_FOLDER"; exit 2; fi
  mount_host=$(readlink -f "$MOUNT_FOLDER")
  echo "Mounting provided folder: $mount_host -> /host_files"
  volume_args+=("$mount_host:/host_files:ro")
  container_file_path="/host_files/$file_name"
elif [[ "$MOUNT_PARENT" = true ]]; then
  parent_host=$(dirname "$FILEPATH")
  mount_host=$(readlink -f "$parent_host")
  echo "Mounting parent folder: $mount_host -> /host_files"
  volume_args+=("$mount_host:/host_files:ro")
  container_file_path="/host_files/$file_name"
else
  echo "Mounting single file: $FILEPATH -> $container_file_path"
  volume_args+=("$FILEPATH:$container_file_path:ro")
fi

# If repo has data folder, mount it
if [[ -d "$repo_root/data" ]]; then
  volume_args+=("$repo_root/data:/data_repo")
fi

# Build image if requested
if [[ "$BUILD" = true ]]; then
  echo "Building image $IMAGE_NAME from docker/Dockerfile.indexer..."
  docker build -f "$repo_root/docker/Dockerfile.indexer" -t "$IMAGE_NAME" "$repo_root"
fi

# Construct docker run command
cmd=(docker run --rm --name "$IMAGE_NAME")
if [[ -f "$ENV_FILE" ]]; then cmd+=(--env-file "$ENV_FILE"); else echo "Env file '$ENV_FILE' not found; continuing without --env-file"; fi
# attach network only if exists
if docker network inspect "$NETWORK" > /dev/null 2>&1; then cmd+=(--network "$NETWORK"); else echo "Network '$NETWORK' not found; using default bridge"; fi
for v in "${volume_args[@]}"; do cmd+=(-v "$v"); done
cmd+=("$IMAGE_NAME" --path "$container_file_path")

echo "Running: ${cmd[*]}"
"${cmd[@]}"

exit_code=$?
if [[ $exit_code -ne 0 ]]; then
  echo "docker run exited with code $exit_code"; exit $exit_code
fi

echo "Indexing completed"
