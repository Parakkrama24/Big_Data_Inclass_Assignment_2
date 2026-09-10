# Maven Setup Script for Windows
# Run this script in PowerShell (Right-click -> Run with PowerShell)

Write-Host "Setting up Maven..." -ForegroundColor Green

# Define Maven version and paths
$mavenVersion = "3.9.6"
$mavenHome = "$env:USERPROFILE\apache-maven-$mavenVersion"
$mavenZip = "$env:TEMP\apache-maven-$mavenVersion-bin.zip"
$mavenUrl = "https://archive.apache.org/dist/maven/maven-3/$mavenVersion/binaries/apache-maven-$mavenVersion-bin.zip"

# Check if Maven is already installed
if (Test-Path $mavenHome) {
    Write-Host "Maven already exists at $mavenHome" -ForegroundColor Yellow
} else {
    Write-Host "Downloading Maven $mavenVersion..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $mavenUrl -OutFile $mavenZip

    Write-Host "Extracting Maven..." -ForegroundColor Cyan
    Expand-Archive -Path $mavenZip -DestinationPath $env:USERPROFILE -Force

    Remove-Item $mavenZip
    Write-Host "Maven extracted to $mavenHome" -ForegroundColor Green
}

# Add Maven to PATH for this session
$env:MAVEN_HOME = $mavenHome
$env:PATH = "$mavenHome\bin;$env:PATH"

Write-Host "`nMaven setup complete!" -ForegroundColor Green
Write-Host "Maven Home: $mavenHome" -ForegroundColor Cyan
Write-Host "`nTesting Maven installation..." -ForegroundColor Cyan

# Test Maven
& "$mavenHome\bin\mvn.cmd" -version

Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "To use Maven in this session, run:" -ForegroundColor Yellow
Write-Host "`$env:PATH = `"$mavenHome\bin;`$env:PATH`"" -ForegroundColor Cyan
Write-Host "`nOr add it permanently to your system PATH" -ForegroundColor Yellow
Write-Host "========================================"  -ForegroundColor Yellow
