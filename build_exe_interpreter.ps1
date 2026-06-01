$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

python .\prepare_bundle.py
python -m pip install --upgrade pyinstaller

pyinstaller `
  --noconfirm `
  --clean `
  --onefile `
  --windowed `
  --name "Anno Island Interpreter" `
  --add-data "third_party;third_party" `
  .\src\app_interpreter.py

Write-Host "Build terminé: $root\dist\Anno Island Interpreter.exe"
