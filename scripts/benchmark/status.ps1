<#
.SYNOPSIS
    Muestra el estado general y progreso de la matriz experimental.
.EXAMPLE
    .\scripts\benchmark\status.ps1
#>
$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
& python (Join-Path $root 'runner\python\src\orchestration\background_ctl.py') status
exit $LASTEXITCODE

