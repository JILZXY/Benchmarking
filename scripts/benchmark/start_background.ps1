<#
.SYNOPSIS
    Ejecuta las validaciones iniciales e inicia el benchmark en segundo plano.
    Evita iniciar dos ejecuciones simultáneas y conserva el progreso al cerrar la terminal.
.EXAMPLE
    .\scripts\benchmark\start_background.ps1
    .\scripts\benchmark\start_background.ps1 --max-runs 1 --warmup-seconds 5 --measurement-seconds 10 --cooldown-seconds 5
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

& python (Join-Path $root 'runner\python\src\orchestration\background_ctl.py') start @RunnerArgs
exit $LASTEXITCODE

