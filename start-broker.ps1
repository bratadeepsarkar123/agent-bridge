param([switch]$DryRun)

# Identity is this checkout's own root, resolved from the script location. A broker
# counts as ours only when its command line names this root's broker.py (or the
# launcher) as a whole path, so a copy or staged checkout works under any folder
# name and another checkout's broker is never taken for ours.
$root = (Resolve-Path -LiteralPath $PSScriptRoot).ProviderPath.TrimEnd('\')
$statePath = Join-Path $root 'broker-session.json'
$ownBrokerPath = Join-Path $root 'broker.py'
$ownLauncherPath = Join-Path $root 'broker_launcher.py'

function Test-CommandLineNamesPath([string]$commandLine, [string]$path) {
    if ([string]::IsNullOrEmpty($commandLine)) { return $false }
    $haystack = $commandLine.ToLowerInvariant()
    $needle = $path.ToLowerInvariant()
    $index = $haystack.IndexOf($needle, [System.StringComparison]::Ordinal)
    while ($index -ge 0) {
        $end = $index + $needle.Length
        $before = if ($index -eq 0) { ' ' } else { [string]$haystack[$index - 1] }
        $after = if ($end -ge $haystack.Length) { ' ' } else { [string]$haystack[$end] }
        if ($before -match '^[\s"'']$' -and $after -match '^[\s"'']$') { return $true }
        $index = $haystack.IndexOf($needle, $index + 1, [System.StringComparison]::Ordinal)
    }
    return $false
}

function Get-OwnRootProcesses {
    @(Get-CimInstance Win32_Process | Where-Object {
        (Test-CommandLineNamesPath $_.CommandLine $ownBrokerPath) -or
        (Test-CommandLineNamesPath $_.CommandLine $ownLauncherPath)
    })
}

if (Test-Path -LiteralPath $statePath) {
    try {
        $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
        $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$state.pid)"
        if ($existing -and (Test-CommandLineNamesPath $existing.CommandLine $ownBrokerPath)) {
            Write-Output 'Bridge broker already running.'
            exit 0
        }
    } catch { throw 'Cannot verify existing broker state; inspect it before restarting.' }
    throw 'Broker state exists but its process could not be verified; inspect it before restarting.'
}

$ownRunning = @(Get-OwnRootProcesses)
if ($ownRunning.Count -gt 0) {
    throw "A broker or launcher for $root is running (process $($ownRunning[0].ProcessId)) but no broker state exists; inspect it before starting."
}

if ($DryRun) {
    Write-Output "Dry run: no broker state and no broker process for $root. Would start $ownLauncherPath."
    exit 0
}

$process = Start-Process -FilePath 'C:\Program Files\Python310\python.exe' `
    -ArgumentList @($ownLauncherPath) -WorkingDirectory $root `
    -WindowStyle Hidden -PassThru
for ($i = 0; $i -lt 50; $i++) {
    if (Test-Path -LiteralPath $statePath) { Write-Output 'Bridge broker started.'; exit 0 }
    if ($process.HasExited) { throw 'Bridge broker exited during startup.' }
    Start-Sleep -Milliseconds 100
}
throw 'Bridge broker did not become ready.'
