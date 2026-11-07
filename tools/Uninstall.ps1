$serviceName = 'FSIndex'

Write-Information "Attempting to remove $serviceName."

if ($null -eq (Get-Service $serviceName)) {
    Write-Error "Failed to find the $serviceName service."
}

sc.exe delete "$serviceName"

Write-Information "$serviceName removed successfully."