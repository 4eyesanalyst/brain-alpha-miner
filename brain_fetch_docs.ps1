# Logs in to the WorldQuant BRAIN API and downloads documentation + reference data into .\docs
# Credentials are read from the user environment variables BRAIN_EMAIL / BRAIN_PASSWORD.

$ErrorActionPreference = "Stop"
$Api = "https://api.worldquantbrain.com"
$Out = Join-Path $PSScriptRoot "docs"
New-Item -ItemType Directory -Force -Path $Out | Out-Null

$email = [Environment]::GetEnvironmentVariable("BRAIN_EMAIL", "User")
$password = [Environment]::GetEnvironmentVariable("BRAIN_PASSWORD", "User")
if (-not $email -or -not $password) { throw "BRAIN_EMAIL / BRAIN_PASSWORD user environment variables are not set." }

$basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${email}:${password}"))
try {
    $auth = Invoke-WebRequest -Uri "$Api/authentication" -Method Post -UseBasicParsing `
        -Headers @{ Authorization = "Basic $basic" } -SessionVariable session
} catch {
    $resp = $_.Exception.Response
    if ($resp -and $resp.Headers["WWW-Authenticate"] -eq "persona") {
        Write-Output "LOGIN NEEDS EXTRA VERIFICATION. Open this link in your browser and complete it, then rerun:"
        Write-Output ($Api + $resp.Headers["Location"])
        exit 2
    }
    Write-Output "LOGIN FAILED: $($_.Exception.Message)"
    exit 1
}
Write-Output "Logged in (HTTP $($auth.StatusCode))."

function Get-Json($path) {
    (Invoke-WebRequest -Uri "$Api$path" -WebSession $session -UseBasicParsing).Content
}

function Save-Json($path, $file) {
    try {
        $json = Get-Json $path
        [IO.File]::WriteAllText((Join-Path $Out $file), $json, [Text.Encoding]::UTF8)
        Write-Host "saved $file"
        return ($json | ConvertFrom-Json)
    } catch {
        Write-Host "failed $path : $($_.Exception.Message)"
        return $null
    }
}

# Documentation index and every page it references
$tutorials = Save-Json "/tutorials" "tutorials.json"
if ($tutorials) {
    $pagesDir = Join-Path $Out "pages"
    New-Item -ItemType Directory -Force -Path $pagesDir | Out-Null
    foreach ($t in $tutorials.results) {
        foreach ($p in $t.pages) {
            $safe = ($p.id -replace '[^\w\-]', '_')
            Save-Json "/tutorial-pages/$($p.id)" "pages\$safe.json" | Out-Null
        }
    }
}

# Reference data used when writing alphas
Save-Json "/operators" "operators.json" | Out-Null
Save-Json "/users/self" "user_self.json" | Out-Null
Save-Json "/simulations/options" "simulation_options.json" | Out-Null
Write-Output "Done. Files are in $Out"
