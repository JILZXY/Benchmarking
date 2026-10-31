<#
.SYNOPSIS
    Solicita una detención controlada, guarda el estado en checkpoint.json y permite reanudar luego.
.EXAMPLE
    .\scripts\benchmark\stop.ps1
#>
$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
& python (Join-Path $root 'runner\python\src\orchestration\background_ctl.py') stop
exit $LASTEXITCODE

