param(
    [string]$Secret = "dev-secret-key",
    [string]$DataDir = "",
    [string]$UploadDir = "",
    [string]$Venv = ".venv"
)

Set-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Path)

if ($Secret) {
    $env:PAPERQUERY_SECRET = $Secret
}

if ($DataDir) {
    $env:PAPERQUERY_DATA_DIR = (Resolve-Path -Path $DataDir)
}

if ($UploadDir) {
    $env:PAPERQUERY_UPLOAD_DIR = (Resolve-Path -Path $UploadDir)
}

# Activate virtual environment when available
if ($Venv -and $Venv.Trim()) {
    $venvPath = Resolve-Path -Path $Venv -ErrorAction SilentlyContinue
    if (-not $venvPath) {
        $venvPath = Resolve-Path -Path (Join-Path (Get-Location) $Venv) -ErrorAction SilentlyContinue
    }
    if ($venvPath) {
        $activateScript = Join-Path $venvPath "Scripts\Activate.ps1"
        if (Test-Path $activateScript) {
            Write-Host ("Activating virtual environment: {0}" -f $venvPath) -ForegroundColor Green
            . $activateScript
        } else {
            Write-Warning ("Activation script not found: {0}" -f $activateScript)
        }
    } elseif ($Venv) {
        Write-Warning ("Virtual environment not found: {0}" -f $Venv)
    }
}

$env:FLASK_APP = "app"

Write-Host "Launching Keydion..." -ForegroundColor Cyan
Write-Host ("  SECRET:      {0}" -f $env:PAPERQUERY_SECRET)
if ($env:PAPERQUERY_DATA_DIR) {
    Write-Host ("  DATA DIR:    {0}" -f $env:PAPERQUERY_DATA_DIR)
}
if ($env:PAPERQUERY_UPLOAD_DIR) {
    Write-Host ("  UPLOAD DIR:  {0}" -f $env:PAPERQUERY_UPLOAD_DIR)
}

$python = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    $python = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $python = "python"
} else {
    Write-Error "Python interpreter not found. Install Python or activate a virtual environment first."
    exit 1
}

& $python -m flask --app app run --debug
