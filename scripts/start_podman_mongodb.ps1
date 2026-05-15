# Start MongoDB with Podman for local dev (FastAPI on host uses MONGO_URI=mongodb://localhost:27017/).
# Uses compose project "kai-agent" so the network name matches docker/podman-compose.app.yaml (kai-agent_kai-net).
#
# Usage (from repo root):
#   .\scripts\start_podman_mongodb.ps1
#
# Optional: start all infra (mongo + postgres + redis):
#   .\scripts\start_podman_mongodb.ps1 -All
#
# If the VM is already running (Podman Desktop): skip auto-start with -SkipMachineStart

param(
    [switch] $All,
    # Skip `podman machine start` if you already started the VM (e.g. from Podman Desktop).
    [switch] $SkipMachineStart
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$ComposeFile = Join-Path $RepoRoot "docker\podman-compose.yaml"

if (-not (Test-Path $ComposeFile)) {
    throw "Missing compose file: $ComposeFile"
}

Set-Location $RepoRoot

function Ensure-PodmanMachineStarted {
    if ($SkipMachineStart) {
        return
    }
    # Windows/macOS: the Podman CLI talks to a Linux VM; it must be running before compose/ps/version.
    Write-Host "Ensuring Podman machine is running (Windows/macOS)..." -ForegroundColor Cyan
    $prevEa = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $null = & podman machine start 2>&1
    }
    finally {
        $ErrorActionPreference = $prevEa
    }
}

function Show-Podman125Help {
    Write-Host ""
    Write-Host "Podman could not connect to its Linux VM (exit 125). Fix locally, then re-run this script:" -ForegroundColor Yellow
    Write-Host "  1) Open Podman Desktop and start the machine, OR in a terminal run:  podman machine start" -ForegroundColor Gray
    Write-Host "  2) If you have no machine yet:  podman machine init   then   podman machine start" -ForegroundColor Gray
    Write-Host "  3) Verify:  podman info   (should not say 'Cannot connect to Podman')" -ForegroundColor Gray
}

if (Get-Command podman -ErrorAction SilentlyContinue) {
    Ensure-PodmanMachineStarted
    if ($All) {
        Write-Warning "Starts Postgres on host port 5432 and Redis on 6379. Stop any local Postgres/Redis on those ports first."
        Write-Host "Starting MongoDB + Postgres + Redis..." -ForegroundColor Cyan
        & podman compose -p kai-agent -f $ComposeFile up -d
    }
    else {
        Write-Host "Starting MongoDB only on port 27017..." -ForegroundColor Cyan
        & podman compose -p kai-agent -f $ComposeFile up -d mongodb
    }
    if ($LASTEXITCODE -ne 0) {
        if ($LASTEXITCODE -eq 125) {
            Show-Podman125Help
        }
        throw "podman compose exited with code $LASTEXITCODE"
    }
}
elseif (Get-Command podman-compose -ErrorAction SilentlyContinue) {
    Ensure-PodmanMachineStarted
    # Legacy: podman-compose uses -f and -p differently; pass through
    if ($All) {
        & podman-compose -p kai-agent -f $ComposeFile up -d
    }
    else {
        & podman-compose -p kai-agent -f $ComposeFile up -d mongodb
    }
    if ($LASTEXITCODE -ne 0) {
        if ($LASTEXITCODE -eq 125) {
            Show-Podman125Help
        }
        throw "podman-compose exited with code $LASTEXITCODE"
    }
}
else {
    throw "Podman not found. Install Podman Desktop for Windows, then retry."
}

Write-Host ""
Write-Host "Next: podman ps --filter name=mongodb" -ForegroundColor Green
Write-Host "Keep in .env: MONGO_URI=mongodb://localhost:27017/" -ForegroundColor Green
Write-Host ""
Write-Host "Restart your FastAPI backend (uvicorn) so health checks and Mongo clients reconnect." -ForegroundColor Yellow
Write-Host "  Example: stop the running terminal, then: uvicorn app.main:app --reload --host 0.0.0.0 --port 8010" -ForegroundColor Gray
