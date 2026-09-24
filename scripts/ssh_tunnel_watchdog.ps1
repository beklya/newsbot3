# Sprint 5.11 - SSH tunnel watchdog
#
# Long-running loop that keeps the SSH tunnel to VPS Redis alive.
# When the tunnel dies (network drop, sleep/wake, VPS reboot), this restarts
# it within ~15-30 seconds. Local services have built-in Redis reconnect, so
# they recover as soon as the tunnel is back.
#
# Two failure modes handled:
#   1. ssh.exe process exited (crash, network unreachable hit ExitOnForwardFailure)
#      -> ssh.exe gone, we spawn a new one
#   2. ssh.exe process alive but tunnel dead (zombie after network glitch)
#      -> TCP probe to 127.0.0.1:6380 fails, we kill + respawn
#
# Run via:
#   - Task Scheduler at logon (recommended for unattended)
#   - Or manually:  .\scripts\ssh_tunnel_watchdog.ps1
#
# Logs to stdout (visible if run in foreground; suppressed if -WindowStyle Hidden).

# --- Singleton guard: only ONE watchdog may run system-wide. Extra instances
# (spawned by repeated PowerShell profile loads / the racy CIM-based dedup) exit
# immediately. Without this, dozens accumulated, each polling WMI every 15s and
# saturating the WMI provider until every query took minutes (2026-06-14).
$script:__wdMutex = New-Object System.Threading.Mutex($false, 'Global\newsbot3_ssh_tunnel_watchdog')
try { $hasHandle = $script:__wdMutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $hasHandle = $true }
if (-not $hasHandle) { exit 0 }

# Replace with your own user@host; ~/.ssh/config can remap it (e.g. to an overlay-network IP).
$tunnelTarget   = "USER@VPS_HOST"
$localBind      = "127.0.0.1:6380"
$remoteBind     = "127.0.0.1:6379"
$probePort      = 6380
$checkIntervalSec = 15
$probeTimeoutMs = 1500
$logFile        = Join-Path $env:TEMP "newsbot_watchdog.log"
$logMaxBytes    = 1MB

# Resolve ssh.exe path explicitly — `Start-Process -FilePath "ssh"` fails when
# the hidden PowerShell host doesn't inherit a PATH that includes OpenSSH
# (observed 2026-06-05: watchdog loop spammed "Cannot find the specified file").
$sshExe = $null
foreach ($candidate in @(
    "$env:SystemRoot\System32\OpenSSH\ssh.exe",
    "$env:ProgramFiles\OpenSSH\ssh.exe",
    "$env:ProgramFiles\Git\usr\bin\ssh.exe",
    "ssh"  # fallback to PATH lookup
)) {
    if ($candidate -eq "ssh") {
        $resolved = (Get-Command ssh -ErrorAction SilentlyContinue).Source
        if ($resolved) { $sshExe = $resolved; break }
    } elseif (Test-Path $candidate) {
        $sshExe = $candidate; break
    }
}
if (-not $sshExe) { $sshExe = "ssh" }  # last resort — let it fail with log

# Redis PING probe — sends RESP "PING\r\n" and expects "+PONG". Catches zombie
# tunnels where TCP socket exists but data doesn't actually flow (observed
# multiple times 2026-06-01).
$pingPayload    = [System.Text.Encoding]::ASCII.GetBytes("PING`r`n")

$tunnelArgs = @(
    "-N",
    "-L", "${localBind}:${remoteBind}",
    "-o", "ServerAliveInterval=15",   # Send keepalive every 15s
    "-o", "ServerAliveCountMax=2",    # After 2 unanswered keepalives (~30s) — disconnect
    "-o", "ExitOnForwardFailure=yes", # Don't keep zombie when forward broken
    "-o", "ConnectTimeout=10",        # Fail fast on initial connect
    "-o", "StrictHostKeyChecking=accept-new",
    $tunnelTarget
)

function Write-Log {
    param([string]$Message)
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Host $line
    try {
        # Rotate if log exceeds 1MB
        if ((Test-Path $logFile) -and (Get-Item $logFile).Length -gt $logMaxBytes) {
            Move-Item -Path $logFile -Destination "$logFile.1" -Force -ErrorAction Stop
        }
        Add-Content -Path $logFile -Value $line -ErrorAction Stop
    } catch {
        # Logging is best-effort; never let it crash the watchdog loop.
    }
}

function Test-TunnelTcp {
    param([int]$Port)
    $ok = $false
    try {
        $tcp = New-Object Net.Sockets.TcpClient
        $iar = $tcp.BeginConnect('127.0.0.1', $Port, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne($probeTimeoutMs, $false)) {
            $tcp.EndConnect($iar)
            $ok = $tcp.Connected
        }
        $tcp.Close()
    } catch { $ok = $false }
    return $ok
}

function Test-RedisPing {
    # End-to-end Redis PING through tunnel. Catches zombie tunnels where TCP
    # accepts but data doesn't flow. Send "PING\r\n", expect first byte '+'
    # (RESP "+PONG\r\n"). Times out fast.
    param([int]$Port)
    $ok = $false
    $tcp = $null
    try {
        $tcp = New-Object Net.Sockets.TcpClient
        $tcp.ReceiveTimeout = $probeTimeoutMs
        $tcp.SendTimeout = $probeTimeoutMs
        $iar = $tcp.BeginConnect('127.0.0.1', $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne($probeTimeoutMs, $false)) {
            return $false
        }
        $tcp.EndConnect($iar)
        if (-not $tcp.Connected) { return $false }
        $stream = $tcp.GetStream()
        $stream.Write($pingPayload, 0, $pingPayload.Length)
        $stream.Flush()
        # Read first byte synchronously with deadline
        $buf = New-Object byte[] 1
        $bytesRead = $stream.Read($buf, 0, 1)
        if ($bytesRead -eq 1 -and $buf[0] -eq [byte][char]'+') {
            $ok = $true
        }
    } catch {
        $ok = $false
    } finally {
        if ($tcp) { $tcp.Close() }
    }
    return $ok
}

function Get-OurSshProcesses {
    Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like "*${localBind}:${remoteBind}*" }
}

function Start-Tunnel {
    try {
        Write-Log "[watchdog] starting SSH tunnel (ssh=$sshExe)..."
        $proc = Start-Process -FilePath $sshExe -ArgumentList $tunnelArgs -WindowStyle Hidden -PassThru -ErrorAction Stop
        Write-Log "[watchdog] tunnel PID=$($proc.Id)"
        Start-Sleep -Seconds 4  # Give it time to establish
    } catch {
        Write-Log "[watchdog] ERROR spawning ssh ($sshExe): $($_.Exception.Message)"
    }
}

Write-Log "[watchdog] starting (target=$tunnelTarget, local=$localBind, log=$logFile)"

while ($true) {
    try {
        $existing = @(Get-OurSshProcesses)
        $tcpAlive = Test-TunnelTcp -Port $probePort
        # Only run the Redis PING if TCP is alive (otherwise we already know it's dead).
        $redisAlive = if ($tcpAlive) { Test-RedisPing -Port $probePort } else { $false }

        if ($existing.Count -eq 0) {
            # No tunnel process -> start one
            Write-Log "[watchdog] no ssh process found, spawning"
            Start-Tunnel
        } elseif (-not $tcpAlive) {
            # Process exists but TCP dead (zombie after network glitch) -> kill + respawn
            Write-Log "[watchdog] tunnel zombie (TCP dead), killing $($existing.Count) process(es)"
            foreach ($p in $existing) {
                try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch { }
            }
            Start-Sleep -Seconds 2
            Start-Tunnel
        } elseif (-not $redisAlive) {
            # TCP alive but Redis PING fails — tunnel forwards bytes but they
            # don't reach Redis (e.g., the ssh process is stuck after VPS sshd
            # bounce, or buffer is broken). Same fix as zombie: kill + respawn.
            Write-Log "[watchdog] tunnel half-dead (TCP ok, Redis PING failed), killing $($existing.Count) process(es)"
            foreach ($p in $existing) {
                try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch { }
            }
            Start-Sleep -Seconds 2
            Start-Tunnel
        } elseif ($existing.Count -gt 1) {
            # Duplicate tunnels (race during restart) -> keep oldest, kill rest
            $sorted = $existing | Sort-Object CreationDate
            $keep = $sorted | Select-Object -First 1
            $kill = $sorted | Select-Object -Skip 1
            Write-Log "[watchdog] $($existing.Count) duplicate tunnels, keeping PID=$($keep.ProcessId), killing $($kill.Count)"
            foreach ($p in $kill) {
                try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch { }
            }
        }
    } catch {
        # Never let a transient exception (Get-CimInstance hiccup, etc) kill the loop.
        Write-Log "[watchdog] ERROR in loop body: $($_.Exception.Message)"
    }
    # All good — sleep until next check
    Start-Sleep -Seconds $checkIntervalSec
}
