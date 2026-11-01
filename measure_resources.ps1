param([ValidateRange(1,30)][int]$Seconds = 3, [string]$ProjectPath = $PSScriptRoot)

# Read-only snapshot of this project's helper processes; agents themselves are excluded.
$projectRoot = [System.IO.Path]::GetFullPath($ProjectPath)
$helpers = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object {
    $_.CommandLine -and $_.CommandLine.Contains($projectRoot)
})
$before = @{}
foreach ($helper in $helpers) {
    $p = Get-Process -Id $helper.ProcessId -ErrorAction SilentlyContinue
    if ($p) { $before[$helper.ProcessId] = $p.CPU }
}
Start-Sleep -Seconds $Seconds
$rows = @()
foreach ($helper in $helpers) {
    $p = Get-Process -Id $helper.ProcessId -ErrorAction SilentlyContinue
    if ($p -and $before.ContainsKey($helper.ProcessId)) {
        $rows += [pscustomobject]@{
            pid = $p.Id
            working_set_mib = [math]::Round($p.WorkingSet64 / 1MB, 1)
            private_mib = [math]::Round($p.PrivateMemorySize64 / 1MB, 1)
            cpu_seconds = [math]::Round(($p.CPU - $before[$helper.ProcessId]), 3)
        }
    }
}
[pscustomobject]@{ sample_seconds = $Seconds; processes = $rows;
    working_set_mib = [math]::Round((($rows | Measure-Object working_set_mib -Sum).Sum), 1);
    private_mib = [math]::Round((($rows | Measure-Object private_mib -Sum).Sum), 1) } | ConvertTo-Json -Depth 3
