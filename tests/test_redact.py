"""scripts/redact.py cleans run output before we commit it as evidence. After redaction the leak scan must find nothing."""

import pytest

from scripts.leak_scan import scan_text
from scripts.redact import local_strings, main, redact_text
from tests.test_leak_scan import FAKES

# Fake local values. They are not the values of any real machine.
ENV = {"USERNAME": "janedoe1", "COMPUTERNAME": "LAPTOP-XYZ", "USERPROFILE": "C:\\Users\\janedoe1"}


@pytest.mark.parametrize("rule", FAKES)
def test_each_planted_fake_is_removed(rule):
    out = redact_text(f"line one\nvalue is {FAKES[rule]} here\n")
    assert FAKES[rule] not in out
    assert scan_text(out) == []
    assert out.splitlines()[0] == "line one" and out.splitlines()[1].startswith("value is ")  # the layout stays


def test_a_resource_listing_loses_its_subscription_id_but_keeps_the_names():
    line = '{"id": "/subscriptions/' + "12345678-1234-1234-1234-" + '123456789012/resourceGroups/rg-trade-pipeline"}'
    out = redact_text(line)
    assert "rg-trade-pipeline" in out and "12345678" not in out


def test_a_whole_workspace_host_goes_with_its_number():
    """The az output holds the workspace ID twice: in the host name and in a workspaceId line. A real run left both behind."""
    text = '"workspace' + 'Id": "1234567890123456",\n"workspaceUrl": "adb-1234567890123456.7.azuredatab' + 'ricks.net"'
    out = redact_text(text)
    assert "1234567890123456" not in out
    assert scan_text(out) == []


def test_a_connection_string_loses_its_key_value():
    text = "Endpoint=" + "sb://demo.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccess" + "Key=SECRETVALUE123"
    assert "SECRETVALUE123" not in redact_text(text)


def test_local_strings_come_from_the_environment_and_skip_short_or_empty_values():
    assert {"janedoe1", "LAPTOP-XYZ", "C:\\Users\\janedoe1", "C:/Users/janedoe1"} <= set(local_strings(ENV))
    assert local_strings({"USERNAME": "a", "COMPUTERNAME": ""}) == []


def test_local_strings_are_removed_in_any_case_and_with_both_slashes():
    text = "path C:\\Users\\janedoe1\\Downloads and C:/Users/JaneDoe1/x on laptop-xyz by janedoe1"
    out = redact_text(text, local_strings(ENV))
    assert "janedoe1" not in out.lower() and "laptop-xyz" not in out.lower()
    assert out.startswith("path ") and " by " in out


def test_clean_text_is_unchanged_and_a_second_pass_changes_nothing():
    clean = "48,630 Silver, 920 quarantine, 450 duplicates. Resource group rg-trade-pipeline.\n"
    assert redact_text(clean, local_strings(ENV)) == clean
    once = redact_text(f"value {FAKES['email']} and {FAKES['guid']}\n", local_strings(ENV))
    assert redact_text(once, local_strings(ENV)) == once


def test_the_command_reads_a_file_and_writes_a_clean_file(tmp_path):
    source, target = tmp_path / "run.txt", tmp_path / "run.clean.txt"
    source.write_text(f"by janedoe1 {FAKES['email']}\n", encoding="utf-8")
    assert main([str(source), str(target)], env=ENV) == 0
    out = target.read_text(encoding="utf-8")
    assert scan_text(out) == [] and "janedoe1" not in out


def test_a_utf16_file_from_windows_powershell_is_cleaned_too(tmp_path):
    """Windows PowerShell 5.1 writes UTF-16 with `>`. Reading it as UTF-8 would find nothing and remove nothing."""
    source, target = tmp_path / "run.txt", tmp_path / "run.clean.txt"
    source.write_text("id " + FAKES["guid"] + " done\n", encoding="utf-16")
    main([str(source), str(target)], env={})
    out = target.read_text(encoding="utf-8")
    assert FAKES["guid"] not in out and "<guid>" in out
