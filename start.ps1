param([switch]$SkipInstall)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item -LiteralPath '.env.example' -Destination '.env' }
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Failed to create Python virtual environment' }
}
if (-not $SkipInstall) {
    & '.venv/Scripts/python.exe' -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed' }
    Push-Location frontend
    try {
        npm.cmd ci --registry=https://registry.npmjs.org --cache="$PSScriptRoot/.npm-cache"
        if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed' }
        npm.cmd run build
        if ($LASTEXITCODE -ne 0) { throw 'Frontend build failed' }
    } finally { Pop-Location }
}
Write-Host 'Open http://127.0.0.1:8000 in your browser. Ctrl+C stops the service.'
& '.venv/Scripts/python.exe' -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
