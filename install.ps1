$ErrorActionPreference = "Stop"

$Repo = $env:QWHYPER_REPO
if (-not $Repo) { $Repo = "your-org/qwhyper" }
$InstallDir = $env:INSTALL_DIR
if (-not $InstallDir) { $InstallDir = Join-Path $env:USERPROFILE "bin" }

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$Url = "https://github.com/$Repo/releases/latest/download/qwhyper-x86_64-pc-windows-msvc.exe"
$Out = Join-Path $InstallDir "qwhyper.exe"

Invoke-WebRequest -Uri $Url -OutFile $Out
Write-Host "qwhyper installed to $Out"
