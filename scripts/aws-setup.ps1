# Refresh PATH (fixes "aws not recognized" in stale Cursor terminals)
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
$env:Path = "$machinePath;$userPath"

$awsExe = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
if (-not (Test-Path $awsExe)) {
  Write-Host "ERROR: AWS CLI not installed at $awsExe" -ForegroundColor Red
  Write-Host "Install with: winget install Amazon.AWSCLI" -ForegroundColor Yellow
  exit 1
}

# Ensure config exists without UTF-8 BOM (AWS CLI rejects BOM)
$configDir = Join-Path $env:USERPROFILE ".aws"
$configPath = Join-Path $configDir "config"
New-Item -ItemType Directory -Force -Path $configDir | Out-Null
if (-not (Test-Path $configPath)) {
  $utf8NoBom = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText(
    $configPath,
    "[default]`r`nregion = us-east-1`r`noutput = json`r`n",
    $utf8NoBom
  )
}

Write-Host "AWS CLI:" -ForegroundColor Cyan
& $awsExe --version
if ($LASTEXITCODE -ne 0) {
  Write-Host "ERROR: aws failed to start. Config may be corrupt." -ForegroundColor Red
  Write-Host "Deleting and recreating $configPath ..." -ForegroundColor Yellow
  Remove-Item $configPath -Force -ErrorAction SilentlyContinue
  $utf8NoBom = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText(
    $configPath,
    "[default]`r`nregion = us-east-1`r`noutput = json`r`n",
    $utf8NoBom
  )
  & $awsExe --version
}

Write-Host ""
Write-Host "Checking credentials..." -ForegroundColor Cyan
& $awsExe sts get-caller-identity 2>$null
if ($LASTEXITCODE -eq 0) {
  Write-Host "Already logged in." -ForegroundColor Green
  exit 0
}

Write-Host ""
Write-Host "Not logged in. Choose a method:" -ForegroundColor Yellow
Write-Host "  1) aws login     (browser / AWS Console session)  [recommended]"
Write-Host "  2) aws configure (Access Key ID + Secret)"
Write-Host ""
$choice = Read-Host "Enter 1 or 2"
if ($choice -eq "1") {
  & $awsExe login
} else {
  & $awsExe configure
}

Write-Host ""
Write-Host "Verifying..." -ForegroundColor Cyan
& $awsExe sts get-caller-identity
if ($LASTEXITCODE -eq 0) {
  Write-Host ""
  Write-Host "OK. Next:" -ForegroundColor Green
  Write-Host "  cd d:\projects\cloud_backtesting\cloud-backtesting-service"
  Write-Host "  npx cdk bootstrap"
  Write-Host "  npx cdk deploy"
} else {
  Write-Host "Login failed. Re-run this script." -ForegroundColor Red
  exit 1
}
