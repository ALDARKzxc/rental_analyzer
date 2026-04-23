param(
    [switch]$IncludeRuntimeData
)

$workspace = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$excludedRoots = @(
    (Join-Path $workspace "venv"),
    (Join-Path $workspace ".venv")
)

function Is-ExcludedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CandidatePath
    )

    foreach ($root in $excludedRoots) {
        if ($CandidatePath.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }

    return $false
}

function Remove-SafePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    if (-not (Test-Path -LiteralPath $TargetPath)) {
        return
    }

    $resolved = (Resolve-Path -LiteralPath $TargetPath).Path
    if (-not $resolved.StartsWith($workspace, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove path outside workspace: $resolved"
    }

    try {
        Remove-Item -LiteralPath $resolved -Recurse -Force -ErrorAction Stop
        Write-Host "Removed: $resolved"
    }
    catch {
        Write-Warning "Could not remove $resolved : $($_.Exception.Message)"
    }
}

$topLevelDirectories = @(
    "build",
    "dist",
    ".pytest_cache",
    "__pycache__"
)

foreach ($name in $topLevelDirectories) {
    Remove-SafePath -TargetPath (Join-Path $workspace $name)
}

Get-ChildItem -Path $workspace -Recurse -Directory -Force -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -eq "__pycache__" -and -not (Is-ExcludedPath $_.FullName) } |
    ForEach-Object { Remove-SafePath -TargetPath $_.FullName }

Get-ChildItem -Path $workspace -Recurse -File -Force -ErrorAction SilentlyContinue -Include *.pyc,*.pyo |
    Where-Object { -not (Is-ExcludedPath $_.FullName) } |
    ForEach-Object { Remove-SafePath -TargetPath $_.FullName }

$generatedFiles = @(
    ".claude\scheduled_tasks.lock",
    "logs\app.log"
)

foreach ($relativePath in $generatedFiles) {
    Remove-SafePath -TargetPath (Join-Path $workspace $relativePath)
}

if ($IncludeRuntimeData) {
    $runtimeTargets = @(
        "data\rental_analyzer.db",
        "результаты анализа"
    )

    foreach ($relativePath in $runtimeTargets) {
        Remove-SafePath -TargetPath (Join-Path $workspace $relativePath)
    }
}

Write-Host "Workspace cleanup finished."
