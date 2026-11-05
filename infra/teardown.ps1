# Deletes Azure resources of the trade-event pipeline.
# Dry run by default: it prints the commands and deletes nothing. Add -Go to delete.
#   -EventHubsOnly : delete only the Event Hubs namespace (end of each session)
#   (no switch)    : delete the whole resource group (end of the project)
[CmdletBinding()]   # a mistyped switch stops the script. Without this, -EventHubOnly would delete the whole group.
param(
    [switch]$Go,
    [switch]$EventHubsOnly
)
$ErrorActionPreference = 'Stop'

$Rg = 'rg-trade-pipeline'

function Run {
    $cmd = @($args | ForEach-Object { $_ })
    Write-Host ($cmd -join ' ')
    if ($Go) {
        & $cmd[0] $cmd[1..($cmd.Count - 1)]
        if ($LASTEXITCODE) { throw "Command failed (exit $LASTEXITCODE)" }
    }
}

if (-not $Go) { Write-Host "DRY RUN. Nothing is deleted. Add -Go to delete.`n" }

if ($EventHubsOnly) {
    $namespaces = az eventhubs namespace list --resource-group $Rg --query '[].name' -o tsv
    if (-not $namespaces) { Write-Host 'No Event Hubs namespace found. Nothing to delete.'; return }
    foreach ($ns in $namespaces) {
        Run az eventhubs namespace delete --name $ns --resource-group $Rg
    }
}
else {
    # Also deletes the Databricks managed resource group.
    Run az group delete --name $Rg --yes --no-wait
}
