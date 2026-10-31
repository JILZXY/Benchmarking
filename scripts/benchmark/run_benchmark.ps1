<#
.SYNOPSIS
    Ejecuta el orquestador secuencial de la matriz experimental ETICORE DB Benchmark.
.EXAMPLE
    .\scripts\benchmark\run_benchmark.ps1 --dry-run
    .\scripts\benchmark\run_benchmark.ps1
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RunnerArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
& python (Join-Path $root 'runner\python\src\orchestration\runner.py') @RunnerArgs
exit $LASTEXITCODE

