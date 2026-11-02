param(
    [string]$ClaudeSessionId,
    [string]$FreshSessionId,
    [string]$CredentialsDir,
    [string]$Model,
    [ValidateSet('low','medium','high','xhigh','max')][string]$Effort,
    [switch]$PrepareOnly,
    [switch]$CheckServices,
    [Parameter(ValueFromRemainingArguments=$true)][string[]]$ClaudeArgs
)

$python = 'C:\Program Files\Python310\python.exe'
$claudeGuid = [Guid]::Empty
# -FreshSessionId starts a NEW session with a supplied UUID and its own credential.
# -ClaudeSessionId keeps the existing resume behaviour (no credential is set).
$fresh = -not [string]::IsNullOrWhiteSpace($FreshSessionId)
if ($fresh -and -not [string]::IsNullOrWhiteSpace($ClaudeSessionId)) {
    throw 'Use either -ClaudeSessionId (resume) or -FreshSessionId (new session), not both.'
}
if ($fresh) {
    if (-not [Guid]::TryParse($FreshSessionId, [ref]$claudeGuid)) {
        throw 'The fresh session ID must be a UUID.'
    }
    if ([string]::IsNullOrWhiteSpace($CredentialsDir)) {
        throw 'A fresh session needs -CredentialsDir, a private folder that will hold its credential.'
    }
    $ClaudeSessionId = $claudeGuid.ToString()
}
$resume = -not $fresh -and -not [string]::IsNullOrWhiteSpace($ClaudeSessionId)
if (-not $resume -and -not $fresh) { $ClaudeSessionId = [Guid]::NewGuid().ToString() }
if (-not [Guid]::TryParse($ClaudeSessionId, [ref]$claudeGuid)) {
    throw 'The Claude session ID must be a UUID.'
}
$state = Join-Path $PSScriptRoot 'broker-session.json'
if (-not $PrepareOnly -or $CheckServices) {
    $client = Join-Path $PSScriptRoot 'codex_client.py'
    & $python -B $client discover *> $null
    if ($LASTEXITCODE -ne 0) {
        $brokerArgs = @((Join-Path $PSScriptRoot 'broker_launcher.py'))
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
}
$existing = Get-CimInstance Win32_Process -Filter "name = 'claude.exe'" |
    Where-Object { $_.CommandLine -like "*$ClaudeSessionId*" }
if ($existing) { throw 'This Claude session is already running; stop it before resuming with the bridge.' }

# Fresh sessions only: provision one new profile credential. The provisioning
# output (JSON that includes the credential file path) is captured and never printed.
$credentialFile = $null
if ($fresh) {
    $provisionCli = Join-Path $PSScriptRoot 'provision_profile.py'
    $provisionOutput = & $python -B $provisionCli provision $ClaudeSessionId --credentials-dir $CredentialsDir
    $provisionExit = $LASTEXITCODE
    if ($provisionExit -eq 3) {
        throw 'Credential provisioning outcome is unsure. Do not retry this ID; check it with provision_profile.py check. Nothing was launched.'
    }
    if ($provisionExit -ne 0) {
        throw 'Credential provisioning was refused. Nothing was launched.'
    }
    try {
        $credentialFile = [string](($provisionOutput -join "`n") | ConvertFrom-Json).credential_file
    } catch { $credentialFile = $null }
    if ([string]::IsNullOrWhiteSpace($credentialFile)) {
        throw 'Provisioning finished but its result could not be read. Nothing was launched.'
    }
}

$configPath = Join-Path $PSScriptRoot "mcp-$ClaudeSessionId.json"
$server = @{
    mcpServers = @{
        'agent-bridge' = @{
            command = 'C:\Program Files\Python310\python.exe'
            args = @((Join-Path $PSScriptRoot 'claude_adapter.py'), '--session-id', $ClaudeSessionId)
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
$credentialVariables = @('BRIDGE_SESSION_CREDENTIAL_FILE', 'BRIDGE_SESSION_CREDENTIAL_FORMAT')
if ($PrepareOnly) {
    # Names only: the credential path and value are never written out.
    $credentialEnvironment = @()
    if ($fresh) { $credentialEnvironment = $credentialVariables }
    @{ sessionId = $ClaudeSessionId; resume = $resume; fresh = $fresh;
       credentialProvisioned = [bool]$credentialFile;
       credentialEnvironment = $credentialEnvironment;
       arguments = $launchArgs; config = $configPath; launched = $false } | ConvertTo-Json -Compress
    return
}
if (-not $credentialFile) {
    & 'C:\Users\brata\.local\bin\claude.exe' @launchArgs
    return
}
# Session-specific credential: set only for this launch, then restore the caller's values.
$previousValues = @{}
foreach ($name in $credentialVariables) { $previousValues[$name] = [Environment]::GetEnvironmentVariable($name) }
try {
    [Environment]::SetEnvironmentVariable('BRIDGE_SESSION_CREDENTIAL_FILE', $credentialFile)
    [Environment]::SetEnvironmentVariable('BRIDGE_SESSION_CREDENTIAL_FORMAT', 'dpapi')
    & 'C:\Users\brata\.local\bin\claude.exe' @launchArgs
} finally {
    foreach ($name in $credentialVariables) { [Environment]::SetEnvironmentVariable($name, $previousValues[$name]) }
}
