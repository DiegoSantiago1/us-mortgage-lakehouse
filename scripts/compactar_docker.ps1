# Devolve ao Windows o espaço vazio dentro do disco virtual do Docker (docker_data.vhdx).
# O VHDX cresce com o uso e não encolhe sozinho (risco R2 do PLAN).
#
# COMO RODAR: PowerShell como Administrador, com o Docker Desktop aberto e NENHUM
# processamento do projeto rodando:
#   powershell -ExecutionPolicy Bypass -File scripts\compactar_docker.ps1
#
# Não apaga dado nenhum: só marca como livres os blocos que o Linux já liberou
# (fstrim) e depois compacta o arquivo (diskpart compact vdisk).

$ErrorActionPreference = "Stop"
$vhdx = Join-Path $env:LOCALAPPDATA "Docker\wsl\disk\docker_data.vhdx"

$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) { throw "Rode este script como Administrador." }
if (-not (Test-Path $vhdx)) { throw "Não achei $vhdx" }

$antes = (Get-Item $vhdx).Length / 1GB
Write-Host ("Tamanho antes: {0:N1} GB" -f $antes)

Write-Host "1/4 fstrim dentro da VM do Docker (marca os blocos livres)..."
docker run --rm --privileged --pid=host alpine:3 nsenter -t 1 -m -- fstrim -av

Write-Host "2/4 Fechando o Docker Desktop e o WSL..."
Get-Process "Docker Desktop" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep 5
wsl --shutdown
Start-Sleep 5

Write-Host "3/4 Compactando o VHDX (pode levar alguns minutos)..."
$roteiro = Join-Path $env:TEMP "compactar_vhdx.txt"
@"
select vdisk file="$vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit
"@ | Set-Content -Path $roteiro -Encoding ascii
diskpart /s $roteiro
Remove-Item $roteiro

$depois = (Get-Item $vhdx).Length / 1GB
Write-Host ("Tamanho depois: {0:N1} GB (liberados {1:N1} GB)" -f $depois, ($antes - $depois))

Write-Host "4/4 Abrindo o Docker Desktop de novo..."
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
