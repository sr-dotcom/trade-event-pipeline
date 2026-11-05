# Creates the Azure resources for the trade-event pipeline (day 5).
# Dry run by default: it prints the commands and creates nothing. Add -Go to create.
# Sign in first, by hand: az login --tenant <your tenant id>   (this script never signs in)
[CmdletBinding()]
param(
    [switch]$Go,
    [switch]$NoEventHubs,   # Stage 1: storage and workspace only. Event Hubs bills by the hour.
    [string]$Subscription = 'Azure for Students',   # the budget alert lives here. The script stops in any other one.
    [string]$Location = 'francecentral',
    [string]$Owner = 'student',
    [int]$DeleteInDays = 7
)
$ErrorActionPreference = 'Stop'

$Rg = 'rg-trade-pipeline'
$StoragePrefix = 'sttradepipe'
$Workspace = 'dbw-trade-pipeline'
$Hub = 'trade-events'

# Names must be unique in Azure. A 6-character hash of the subscription id makes them unique and stable.
$subName = az account show --query name -o tsv
if (-not $subName) { throw 'Not signed in. Run az login first.' }
if ($subName -ne $Subscription) {
    throw "The default subscription is '$subName'. Expected '$Subscription'. Run: az account set --subscription '$Subscription'"
}
$subId = az account show --query id -o tsv
$sha = [System.Security.Cryptography.SHA1]::Create()
$suffix = -join ($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($subId))[0..2] | ForEach-Object { $_.ToString('x2') })
$St = "$StoragePrefix$suffix"
$Ns = "evh-trade-$suffix"

$DeleteBy = (Get-Date).AddDays($DeleteInDays).ToString('yyyy-MM-dd')
$Tags = @('project=trade-event-pipeline', "owner=$Owner", "delete-by=$DeleteBy")

function Run {
    $cmd = @($args | ForEach-Object { $_ })
    Write-Host ($cmd -join ' ')
    if ($Go) {
        & $cmd[0] $cmd[1..($cmd.Count - 1)]
        if ($LASTEXITCODE) { throw "Command failed (exit $LASTEXITCODE)" }
    }
}

if (-not $Go) { Write-Host "DRY RUN. Nothing is created. Add -Go to create.`n" }

Run az group create --name $Rg --location $Location --tags $Tags
Run az storage account create --name $St --resource-group $Rg --location $Location --sku Standard_LRS --kind StorageV2 --hns true --tags $Tags
Run az storage container create --name archive --account-name $St --auth-mode key
if (-not $NoEventHubs) {
Run az eventhubs namespace create --name $Ns --resource-group $Rg --location $Location --sku Standard --capacity 1 --enable-kafka true --tags $Tags
Run az eventhubs eventhub create --name $Hub --namespace-name $Ns --resource-group $Rg --partition-count 2 --enable-capture true --capture-interval 60 --skip-empty-archives true --destination-name EventHubArchive.AzureBlockBlob --storage-account $St --blob-container archive
}
Run az databricks workspace create --name $Workspace --resource-group $Rg --location $Location --sku premium --tags $Tags

if ($NoEventHubs) {
    Write-Host "`nNames: group=$Rg storage=$St workspace=$Workspace delete-by=$DeleteBy"
} else {
    Write-Host "`nNames: group=$Rg storage=$St namespace=$Ns hub=$Hub workspace=$Workspace delete-by=$DeleteBy"
    Write-Host 'Delete Event Hubs at the end of each session: .\infra\teardown.ps1 -EventHubsOnly -Go'
}
