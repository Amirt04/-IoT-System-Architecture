# Lab 2: реальное время подключения ноутбука к Wi-Fi (ассоциация + аутентификация + DHCP)
# Запуск в PowerShell из папки lab2:   powershell -ExecutionPolicy Bypass -File tools\wifi_connect_time.ps1
# ВНИМАНИЕ: на каждый замер Wi-Fi отключается на несколько секунд (интернет пропадёт и вернётся).

param([int]$Runs = 5)

$ifc = netsh wlan show interfaces
$name = ($ifc | Select-String '^\s*(Name|Имя)\s*:\s*(.+)$' | Select-Object -First 1).Matches[0].Groups[2].Value.Trim()
$ssidMatch = $ifc | Select-String '^\s*SSID\s*:\s*(.+)$' | Select-Object -First 1
if (-not $ssidMatch) { Write-Host "Wi-Fi не подключён — подключись к сети и запусти снова"; exit 1 }
$ssid = $ssidMatch.Matches[0].Groups[1].Value.Trim()
$sig = ($ifc | Select-String '^\s*(Signal|Сигнал)\s*:\s*(\d+)\s*%' | Select-Object -First 1).Matches[0].Groups[2].Value
$dbm = [int]$sig / 2 - 100
Write-Host "Интерфейс: $name, сеть: $ssid, сигнал: $sig % (~$dbm dBm)"

$out = Join-Path $PSScriptRoot "..\results\wifi_connect.csv"
New-Item -ItemType Directory -Force -Path (Split-Path $out) | Out-Null
"run,signal_pct,rssi_dbm_est,link_up_ms,ip_ready_ms" | Set-Content -Encoding UTF8 $out

for ($i = 1; $i -le $Runs; $i++) {
    netsh wlan disconnect interface="$name" | Out-Null
    Start-Sleep -Seconds 4
    $sw = [Diagnostics.Stopwatch]::StartNew()
    netsh wlan connect name="$ssid" interface="$name" | Out-Null

    # 1) канал поднят: ассоциация + WPA-аутентификация завершены
    do { Start-Sleep -Milliseconds 20 } until ((Get-NetAdapter -Name $name).Status -eq 'Up' -or $sw.Elapsed.TotalSeconds -gt 30)
    $tLink = $sw.Elapsed.TotalMilliseconds

    # 2) есть IP и шлюз отвечает: DHCP завершён, можно открывать TCP
    $ready = $false
    do {
        Start-Sleep -Milliseconds 20
        $gw = (Get-NetIPConfiguration -InterfaceAlias $name -ErrorAction SilentlyContinue).IPv4DefaultGateway.NextHop
        if ($gw) { ping -n 1 -w 300 $gw | Out-Null; $ready = ($LASTEXITCODE -eq 0) }
    } until ($ready -or $sw.Elapsed.TotalSeconds -gt 30)
    $tIp = $sw.Elapsed.TotalMilliseconds

    Write-Host ("Замер {0}: канал {1:N0} мс, IP+шлюз {2:N0} мс" -f $i, $tLink, $tIp)
    "$i,$sig,$dbm,$([int]$tLink),$([int]$tIp)" | Add-Content -Encoding UTF8 $out
    Start-Sleep -Seconds 3
}
Write-Host "Результат: $out"
