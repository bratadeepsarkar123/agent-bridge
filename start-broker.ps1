$statePath = Join-Path $PSScriptRoot 'broker-session.json'
if (Test-Path -LiteralPath $statePath) {
    try {
        $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
        $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$state.pid)"
        if ($existing -and $existing.CommandLine -like '*agent-bridge-experiment*broker.py*') {
            Write-Output 'Bridge broker already running.'
            exit 0
        }
    } catch { throw 'Cannot verify existing broker state; inspect it before restarting.' }
    throw 'Broker state exists but its process could not be verified; inspect it before restarting.'
}
$process = Start-Process -FilePath 'C:\Program Files\Python310\python.exe' `
    -ArgumentList @((Join-Path $PSScriptRoot 'broker.py')) -WorkingDirectory $PSScriptRoot `
    -WindowStyle Hidden -PassThru
for ($i = 0; $i -lt 50; $i++) {
    if (Test-Path -LiteralPath $statePath) { Write-Output 'Bridge broker started.'; exit 0 }
    if ($process.HasExited) { throw 'Bridge broker exited during startup.' }
    Start-Sleep -Milliseconds 100
}
throw 'Bridge broker did not become ready.'
