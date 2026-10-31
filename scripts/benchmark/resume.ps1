<#
.SYNOPSIS
    Recupera el estado anterior desde checkpoint.json y continúa las ejecuciones pendientes en segundo plano.
.EXAMPLE
    .\scripts\benchmark\resume.ps1
    .\scripts\benchmark\resume.ps1 --retry-errors
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

& python (Join-Path $root 'runner\python\src\orchestration\background_ctl.py') resume @RunnerArgs
exit $LASTEXITCODE

