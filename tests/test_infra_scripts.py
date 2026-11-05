"""Rules for the Azure scripts in infra/. These tests read the text only. They create nothing."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1] / "infra"
SETUP = (INFRA / "setup.ps1").read_text(encoding="utf-8") if (INFRA / "setup.ps1").exists() else ""
TEARDOWN = (INFRA / "teardown.ps1").read_text(encoding="utf-8") if (INFRA / "teardown.ps1").exists() else ""


def test_scripts_exist():
    assert SETUP and TEARDOWN


def test_every_create_has_the_three_tags():
    creates = [ln for ln in SETUP.splitlines() if re.search(r"\baz\b.* create\b", ln)]
    assert len(creates) >= 5  # group, storage, container, event hubs (namespace and hub), workspace
    # The event hub and the container are children. They take no tags. Everything else must.
    for ln in creates:
        if "eventhubs eventhub create" in ln or "storage container create" in ln:
            continue
        assert "$Tags" in ln, ln
    tags_line = next(ln for ln in SETUP.splitlines() if ln.strip().startswith("$Tags"))
    for key in ("project=", "owner=", "delete-by="):
        assert key in tags_line


def test_setup_is_a_dry_run_unless_go():
    assert "[switch]$Go" in SETUP
    assert "[switch]$Go" in TEARDOWN


def test_event_hubs_standard_with_kafka_and_capture():
    assert "--sku Standard" in SETUP
    assert "--enable-kafka true" in SETUP
    assert "--enable-capture true" in SETUP


def test_storage_name_rule():
    prefix = re.search(r"\$StoragePrefix\s*=\s*'([^']+)'", SETUP).group(1)
    assert re.fullmatch(r"[a-z0-9]+", prefix)
    assert len(prefix) + 6 <= 24  # 6 chars are added at run time


def test_no_secrets_in_scripts():
    for text in (SETUP, TEARDOWN):
        assert not re.search(r"Account" r"Key=|SharedAccess" r"Key=|-Password|--password|list-keys", text, re.IGNORECASE)
        assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text, re.IGNORECASE)


def test_teardown_deletes_the_group_and_can_delete_only_event_hubs():
    assert "az group delete" in TEARDOWN
    assert "az eventhubs namespace delete" in TEARDOWN
    assert "[switch]$EventHubsOnly" in TEARDOWN


def test_scripts_do_not_log_in():
    for text in (SETUP, TEARDOWN):
        code = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
        assert not any(re.search(r"\baz login\b", ln) and "throw" not in ln for ln in code)


def dry_run(*args):
    """Run setup.ps1 as a dry run with a fake `az` function. The fake answers only `az account show`. Nothing is created."""
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    command = f"function az {{ 'fake-subscription' }}; & '{INFRA / 'setup.ps1'}' -Subscription fake-subscription {' '.join(args)}"
    done = subprocess.run([shell, "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode == 0, done.stderr
    return done.stdout


def test_dry_run_lists_event_hubs_by_default():
    out = dry_run()
    assert "az eventhubs namespace create" in out
    assert "az databricks workspace create" in out


def test_no_event_hubs_switch_leaves_out_every_event_hubs_command():
    out = dry_run("-NoEventHubs")
    assert "eventhubs" not in out
    for kept in ("az group create", "az storage account create", "az storage container create", "az databricks workspace create"):
        assert kept in out


def test_only_event_hubs_switch_adds_the_hub_to_an_existing_group_and_names_the_capture_role():
    """Stage 2 creates storage first, the user grants the Capture role, then the hub. See docs for the Capture permission."""
    out = dry_run("-OnlyEventHubs")
    for kept in ("az eventhubs namespace create", "az eventhubs eventhub create"):
        assert kept in out
    for left_out in ("az group create", "az storage account create", "az storage container create", "az databricks workspace create"):
        assert left_out not in out
    assert "Storage Blob Data Owner" in out  # the user must hold this role first, or Capture fails with "Linked access check failed"


def test_the_two_hub_switches_cannot_be_used_together():
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    command = f"function az {{ 'fake-subscription' }}; & '{INFRA / 'setup.ps1'}' -Subscription fake-subscription -NoEventHubs -OnlyEventHubs"
    done = subprocess.run([shell, "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode != 0 and "az eventhubs" not in done.stdout


def test_an_unknown_switch_stops_the_script_instead_of_being_ignored():
    """Without this, a typo such as -NoEventHubz would run the full setup and start Event Hubs billing."""
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    command = f"function az {{ 'fake-subscription' }}; & '{INFRA / 'setup.ps1'}' -NoEventHubz"
    done = subprocess.run([shell, "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode != 0 and "az eventhubs" not in done.stdout


def test_setup_refuses_to_run_in_the_wrong_subscription():
    """The budget alert lives in one subscription. The CLI default may be another one. The fake az reports 'fake-subscription'."""
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    command = f"function az {{ 'fake-subscription' }}; & '{INFRA / 'setup.ps1'}'"  # default expected name: Azure for Students
    done = subprocess.run([shell, "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode != 0
    assert "az group create" not in done.stdout
    assert "Azure for Students" in done.stderr


def run_teardown(*args):
    """Run teardown.ps1 with a fake `az` function. It prints what it would delete. Nothing is deleted."""
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    command = f"function az {{ 'fake-namespace' }}; & '{INFRA / 'teardown.ps1'}' {' '.join(args)}"
    return subprocess.run([shell, "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=120, check=False)


def test_teardown_dry_run_lists_the_group_delete():
    done = run_teardown()
    assert done.returncode == 0 and "az group delete" in done.stdout


def test_a_mistyped_teardown_switch_stops_instead_of_deleting_the_whole_group():
    """Without strict binding, -EventHubOnly (no s) is ignored and the script falls through to `az group delete`."""
    done = run_teardown("-EventHubOnly")
    assert done.returncode != 0
    assert "az group delete" not in done.stdout
