<#
.SYNOPSIS
    Genera el dataset experimental de intentos y su manifiesto SHA-256.
.EXAMPLE
    .\scripts\data\generate_dataset.ps1
    .\scripts\data\generate_dataset.ps1 --count 1000 --seed 20261030
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$GeneratorArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
& python (Join-Path $root 'dataset\generator\generate.py') @GeneratorArgs
exit $LASTEXITCODE

