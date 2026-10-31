<#
.SYNOPSIS
    Ejecuta las validaciones iniciales (Docker + dataset SHA-256) y corre la matriz de benchmarks.
.EXAMPLE
    .\scripts\benchmark\run_benchmarks.ps1
    .\scripts\benchmark\run_benchmarks.ps1 --resume
    .\scripts\benchmark\run_benchmarks.ps1 --dry-run
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RunnerArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')

$envFile = Join-Path $root '.env'
$envExample = Join-Path $root '.env.example'
if (-not (Test-Path $envFile) -and (Test-Path $envExample)) {
    Copy-Item $envExample $envFile
}

& python (Join-Path $root 'scripts\setup\verify_docker_env.py')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& python (Join-Path $root 'dataset\generator\generate.py') --verify-only
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& python (Join-Path $root 'runner\python\src\orchestration\runner.py') @RunnerArgs
exit $LASTEXITCODE

