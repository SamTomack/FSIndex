$serviceName = 'FSIndex'

Write-Information "Thank you for installing $serviceName!"
$path = Read-Host "Enter the path to the $serviceName executable"

if ($path -notmatch "^.+\\$serviceName\.exe" -or -not (Test-Path $path)) {
    Write-Error "Failed to validate $serviceName path. Exiting."
}

$svcParams = @{
    Name            = $serviceName
    BinaryPathName  = $path
    Description     = "A local file indexer for Windows."
    StartupType     = "Automatic"
}

New-Service @params

Write-Information "$serviceName installed successfully."