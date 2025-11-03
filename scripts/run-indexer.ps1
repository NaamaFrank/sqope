<#
Run this from PowerShell anywhere; the script will resolve the repository root based on the script location.
Usage examples:
    # mount a single PDF and index it
    .\scripts\run-indexer.ps1 -FilePath "C:\full\path\to\your.pdf" -DocId your_doc_id

    # mount the file's parent folder (so many files in same folder are available)
    .\scripts\run-indexer.ps1 -FilePath "C:\full\path\to\your.pdf" -DocId your_doc_id -MountParent

    # mount an arbitrary folder instead of a single file
    .\scripts\run-indexer.ps1 -FilePath "C:\full\path\to\your.pdf" -DocId your_doc_id -MountFolder "C:\some\folder"

Options:
  -Build: build the Docker image first using docker/Dockerfile.indexer if the image doesn't exist
  -ImageName: name of the docker image (default: sqope-indexer)
  -Network: docker network to use (default: sqope_default)
  -EnvFile: path to env file to pass into container (default: .env)

This script resolves the host path, mounts it into the container under /data, and calls
`python -m indexer file --path /data/<filename> --doc-id <docid>` inside the container.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath,

    [Parameter(Mandatory=$true)]
    [string]$DocId,

    [string]$ImageName = "sqope-indexer",
    [string]$Network = "sqope_default",
    [string]$EnvFile = ".env",
    [switch]$Build,
    [switch]$MountParent,
    [string]$MountFolder = ""
)

function Write-Info($m) {
    
    if ($PSBoundParameters.ContainsKey('Verbose')) {
        Write-Host $m -ForegroundColor Cyan
    } else {
        Write-Host $m
    }
}

# Resolve path
try {
    $resolved = Resolve-Path -Path $FilePath -ErrorAction Stop
    $absPath = $resolved.ProviderPath
} catch {
    Write-Error "File not found: $FilePath"
    exit 2
}

if (-not (Test-Path $absPath -PathType Leaf)) {
    Write-Error "Not a file: $absPath"
    exit 2
}

$fileName = Split-Path -Path $absPath -Leaf
$containerFilePath = "/data/$fileName"

# Determine mount behavior: single file, parent folder, or arbitrary folder
$volumeArgs = @()
if ($MountFolder -ne "") {
    if (-not (Test-Path $MountFolder)) { Write-Error "Mount folder not found: $MountFolder"; exit 2 }
    $mountHost = (Resolve-Path $MountFolder).ProviderPath
    Write-Info "Mounting provided folder: $mountHost -> /host_files"
    $volumeArgs += ($mountHost + ':/host_files:ro')
    $containerFilePath = "/host_files/$fileName"
} elseif ($MountParent) {
    $parentHost = Split-Path -Path $absPath -Parent
    Write-Info "Mounting parent folder: $parentHost -> /host_files"
    $volumeArgs += ($parentHost + ':/host_files:ro')
    $containerFilePath = "/host_files/$fileName"
} else {
    Write-Info "Mounting single file: $absPath -> $containerFilePath"
    $volumeArgs += ($absPath + ':' + $containerFilePath + ':ro')
}

# Add optional data folder mount if present in repo
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
# repo root is the parent of the scripts/ folder (script lives in repo/scripts)
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).ProviderPath
$dataHost = Join-Path $repoRoot "data"
if (Test-Path $dataHost) {
    Write-Info "Mounting repo data folder: $dataHost -> /data_repo"
    $volumeArgs += ($dataHost + ':/data_repo')
}

# Check if image exists
$imageExists = $false
try {
    & docker image inspect $ImageName > $null 2>&1
    if ($LASTEXITCODE -eq 0) { $imageExists = $true }
} catch {
    $imageExists = $false
}

if (-not $imageExists) {
    if ($Build) {
        Write-Host "Image '$ImageName' not found. Building from docker/Dockerfile.indexer..."
        $buildCmd = "docker build -f docker/Dockerfile.indexer -t $ImageName ."
        Write-Host $buildCmd
        $b = Invoke-Expression $buildCmd
        if ($LASTEXITCODE -ne 0) { Write-Error "docker build failed"; exit 3 }
    } else {
        Write-Warning "Docker image '$ImageName' not found. Run with -Build to build it, or build the image yourself."
    }
}

# If requested network doesn't exist, warn (but still allow running with default bridge)
$networkExists = $false
try {
    $networks = & docker network ls --format "{{.Name}}"
    if ($networks -match [regex]::Escape($Network)) { $networkExists = $true }
} catch {
    $networkExists = $false
}

$dockerArgs = @('run','--rm')
# Create a friendly ephemeral container name
$containerName = "sqope-indexer-$DocId"
$dockerArgs += @('--name',$containerName)
if (Test-Path $EnvFile) {
    $dockerArgs += @('--env-file',$EnvFile)
} else {
    Write-Info "Env file '$EnvFile' not found; continuing without --env-file"
}
if ($networkExists) { $dockerArgs += @('--network',$Network) } else { Write-Info "Network '$Network' not found; using default bridge network" }

# Add volume mounts
foreach ($v in $volumeArgs) { $dockerArgs += @('-v',$v) }

# Final invocation: image then subcommand and args
$useSubcommand = $false
try {
    # Try running `python -m indexer file --help` inside the image. If it exits 0, the subcommand exists.
    & docker run --rm --entrypoint python $ImageName -m indexer file --help > $null 2>&1
    if ($LASTEXITCODE -eq 0) { $useSubcommand = $true }
} catch {
    $useSubcommand = $false
}

$dockerArgs += $ImageName
if ($useSubcommand) {
    $dockerArgs += 'file'
}
$dockerArgs += '--path'
$dockerArgs += $containerFilePath
$dockerArgs += '--doc-id'
$dockerArgs += $DocId

Write-Host "Running: docker $($dockerArgs -join ' ')" -ForegroundColor Green

# Execute docker with the constructed args using the call operator so arguments with colons are passed intact
& docker @dockerArgs
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) { Write-Error "docker run exited with code $exitCode"; exit $exitCode }

Write-Host "Indexing completed (container: $containerName)"
