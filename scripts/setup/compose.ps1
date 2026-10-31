<#
.SYNOPSIS
    Wrapper de docker compose que carga .env + el perfil experimental indicado.

.EXAMPLE
    .\scripts\setup\compose.ps1 c --profile postgresql up -d
    .\scripts\setup\compose.ps1 a --profile "*" down
#>
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('a', 'b', 'c')]
    [string]$ResourceProfile,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ComposeArgs
)

$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')

$profileFile = @{
    'a' = 'profile_a_baseline.env'
    'b' = 'profile_b_medium.env'
    'c' = 'profile_c_severe.env'
}[$ResourceProfile.ToLower()]

$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) {
    throw "No existe $envFile. Copiar .env.example a .env primero."
}

# docker compose escribe su progreso en stderr; con 'Stop', Windows PowerShell
# lo trataría como error al redirigir la salida (2>&1).
$ErrorActionPreference = 'Continue'
& docker compose `
    --project-directory $root `
    --env-file $envFile `
    --env-file (Join-Path $root "config\profiles\$profileFile") `
    @ComposeArgs
exit $LASTEXITCODE
