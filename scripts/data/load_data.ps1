<#
.SYNOPSIS
    Carga el dataset de intentos en MongoDB o PostgreSQL y verifica su integridad.
.EXAMPLE
    .\scripts\data\load_data.ps1 mongodb --dataset dataset/fixtures/intentos_sample_100.jsonl --manifest dataset/manifests/intentos_sample_100.manifest.json
    .\scripts\data\load_data.ps1 postgresql --dataset dataset/fixtures/intentos_sample_100.jsonl --manifest dataset/manifests/intentos_sample_100.manifest.json
    .\scripts\data\load_data.ps1 verify --engine both --dataset dataset/fixtures/intentos_sample_100.jsonl --manifest dataset/manifests/intentos_sample_100.manifest.json
#>
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('mongodb', 'postgresql', 'verify')]
    [string]$Action,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$LoaderArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')

$scriptMap = @{
    'mongodb'    = 'runner/src/load/mongodb_loader.py'
    'postgresql' = 'runner/src/load/postgresql_loader.py'
    'verify'     = 'runner/src/load/verify_load.py'
}

& python -c "import pymongo, psycopg" *> $null
if ($LASTEXITCODE -eq 0) {
    $hostMap = @{
        'mongodb'    = 'runner\python\src\load\mongodb_loader.py'
        'postgresql' = 'runner\python\src\load\postgresql_loader.py'
        'verify'     = 'runner\python\src\load\verify_load.py'
    }
    & python (Join-Path $root $hostMap[$Action.ToLower()]) @LoaderArgs
    exit $LASTEXITCODE
} else {
    & (Join-Path $root 'scripts\setup\compose.ps1') a --profile runner run --rm runner python ("/app/" + $scriptMap[$Action.ToLower()]) @LoaderArgs
    exit $LASTEXITCODE
}

