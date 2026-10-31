<#
.SYNOPSIS
    Captura y guarda los planes de ejecución (EXPLAIN) de Q1 y Q2 para el motor activo.
.EXAMPLE
    .\scripts\evidence\capture_explain.ps1 --engine postgresql --profile PROFILE_C_SEVERE
    .\scripts\evidence\capture_explain.ps1 --engine mongodb --profile PROFILE_C_SEVERE
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExplainArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')

& python -c "import pymongo, psycopg" *> $null
if ($LASTEXITCODE -eq 0) {
    & python (Join-Path $root 'runner\python\src\adapters\explain_runner.py') @ExplainArgs
    exit $LASTEXITCODE
} else {
    & (Join-Path $root 'scripts\setup\compose.ps1') c --profile runner run --rm runner python /app/runner/src/adapters/explain_runner.py @ExplainArgs
    exit $LASTEXITCODE
}

