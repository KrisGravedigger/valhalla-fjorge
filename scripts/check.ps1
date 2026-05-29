$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $ProjectRoot
try {
    Write-Host "== py_compile: valhalla tools =="
    New-Item -ItemType Directory -Force -Path "_temp" | Out-Null
    $CompileOutput = Join-Path (Resolve-Path "_temp") "compile-check.pyc"
    $CompileFiles = Get-ChildItem -Path "valhalla", "tools" -Recurse -Filter "*.py" | ForEach-Object { $_.FullName }
    python -c "import py_compile, sys; cfile=sys.argv[1]; [py_compile.compile(path, cfile=cfile, doraise=True) for path in sys.argv[2:]]" $CompileOutput @CompileFiles
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host "== pytest: tests/ =="
    python -m pytest tests/
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host "== baseline: --report =="
    python tests/verify_baseline.py --report
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}
