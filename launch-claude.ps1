param(
    [string]$ClaudeSessionId,
    [string]$CodexSessionId = '01a18ed9-5766-7ef1-9328-67ed7465809a',
    [string]$Model,
    [ValidateSet('low','medium','high','xhigh','max')][string]$Effort,
    [switch]$PrepareOnly,
    [switch]$CheckServices,
    [Parameter(ValueFromRemainingArguments=$true)][string[]]$ClaudeArgs
)

$claudeGuid = [Guid]::Empty
$codexGuid = [Guid]::Empty
$resume = -not [string]::IsNullOrWhiteSpace($ClaudeSessionId)
if (-not $resume) { $ClaudeSessionId = [Guid]::NewGuid().ToString() }
if (-not [Guid]::TryParse($ClaudeSessionId, [ref]$claudeGuid) -or
    -not [Guid]::TryParse($CodexSessionId, [ref]$codexGuid)) {
    throw 'Session IDs must be UUIDs.'
}
$state = Join-Path $PSScriptRoot 'broker-session.json'
if (-not $PrepareOnly -or $CheckServices) {
    $python = 'C:\Program Files\Python310\python.exe'
    $client = Join-Path $PSScriptRoot 'codex_client.py'
    & $python -B $client discover *> $null
    if ($LASTEXITCODE -ne 0) {
        $brokerArgs = @((Join-Path $PSScriptRoot 'broker.py'))
        if (Test-Path -LiteralPath $state) {
            try {
                $brokerState = Get-Content -LiteralPath $state -Raw | ConvertFrom-Json
                $oldPid = [int]$brokerState.pid
                if ($oldPid -le 0 -or (Get-Process -Id $oldPid -ErrorAction SilentlyContinue)) {
                    throw 'The broker state cannot safely be recovered automatically.'
                }
            } catch { throw 'The bridge broker is unavailable and its state could not be safely recovered.' }
            $brokerArgs += '--recover'
        }
        Start-Process -FilePath $python -ArgumentList $brokerArgs -WorkingDirectory $PSScriptRoot -WindowStyle Hidden | Out-Null
        $ready = $false
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            Start-Sleep -Milliseconds 250
            & $python -B $client discover *> $null
            if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        }
        if (-not $ready) { throw 'The bridge broker could not be started.' }
    }
    & $python -B $client register --id $CodexSessionId --kind codex *> $null
    if ($LASTEXITCODE -ne 0) { throw 'The destination Codex session could not be registered.' }
    $workerState = Join-Path $PSScriptRoot "native-worker-$CodexSessionId.json"
    $workerRunning = $false
    if (Test-Path -LiteralPath $workerState) {
        try {
            $worker = Get-Content -LiteralPath $workerState -Raw | ConvertFrom-Json
            $workerPid = [int]$worker.pid
            $process = Get-CimInstance Win32_Process -Filter "ProcessId = $workerPid"
            if ($process) {
                if ($process.CommandLine -notlike "*native_worker.py*$CodexSessionId*") {
                    throw 'The worker process cannot safely be verified.'
                }
                $workerRunning = $true
            }
        } catch { throw 'The Codex bridge worker state could not be safely verified.' }
    }
    if (-not $workerRunning) {
        $workerArgs = @('-B', (Join-Path $PSScriptRoot 'native_worker.py'), '--recipient', $CodexSessionId)
        Start-Process -FilePath $python -ArgumentList $workerArgs -WorkingDirectory $PSScriptRoot -WindowStyle Hidden | Out-Null
    }
}
$existing = Get-CimInstance Win32_Process -Filter "name = 'claude.exe'" |
    Where-Object { $_.CommandLine -like "*$ClaudeSessionId*" }
if ($existing) { throw 'This Claude session is already running; stop it before resuming with the bridge.' }

$configPath = Join-Path $PSScriptRoot "mcp-$ClaudeSessionId.json"
$server = @{
    mcpServers = @{
        'agent-bridge' = @{
            command = 'C:\Program Files\Python310\python.exe'
            args = @((Join-Path $PSScriptRoot 'claude_adapter.py'), '--session-id',
                $ClaudeSessionId, '--codex-id', $CodexSessionId)
        }
    }
}
[System.IO.File]::WriteAllText($configPath, ($server | ConvertTo-Json -Depth 6),
    [System.Text.UTF8Encoding]::new($false))
$launchArgs = @()
if ($resume) { $launchArgs += @('--resume', $ClaudeSessionId) }
else { $launchArgs += @('--session-id', $ClaudeSessionId) }
$launchArgs += @('--mcp-config', $configPath, '--dangerously-load-development-channels', 'server:agent-bridge')
if ($Model) { $launchArgs += @('--model', $Model) }
if ($Effort) { $launchArgs += @('--effort', $Effort) }
if ($ClaudeArgs) { $launchArgs += $ClaudeArgs }
if ($PrepareOnly) {
    @{ sessionId = $ClaudeSessionId; resume = $resume; arguments = $launchArgs; config = $configPath } | ConvertTo-Json -Compress
    return
}
& 'C:\Users\brata\.local\bin\claude.exe' @launchArgs
