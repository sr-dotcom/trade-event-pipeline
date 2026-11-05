"""Rules for scripts/leak_scan.py. The fake secrets are built from two parts, so this file does not trip the scan."""
import pytest

from scripts.leak_scan import scan_text, scan_tracked

FAKES = {
    "guid": "12345678-1234-1234-1234-" + "123456789012",
    "shared_access_key": "SharedAccess" + "Key=abcd1234",
    "account_key": "Account" + "Key=abcd1234",
    "connection_string": "Endpoint=" + "sb://demo.servicebus.windows.net/",
    "databricks_url": "adb-1.1.azuredatab" + "ricks.net",
    "email": "someone" + "@example.com",
    "databricks_token": "dapi" + "0123456789abcdef0123456789abcdef",
    "databricks_workspace_id": '"workspace' + 'Id": "1234567890123456"',
    "account_puid": '"pu' + 'id": "00030000AABBCCDD"',
}


@pytest.mark.parametrize("rule", FAKES)
def test_each_planted_fake_is_found(rule):
    found = scan_text(f"line one\nvalue is {FAKES[rule]} here\n")
    assert (rule, 2) in found


def test_clean_text_has_no_findings():
    text = "Use az login --tenant <tenant id>.\nThe scan looks for SharedAccessKey and .azuredatabricks.net.\n"
    assert scan_text(text) == []


def test_findings_do_not_carry_the_secret():
    for finding in scan_text(FAKES["account_key"]):
        assert all(FAKES["account_key"] not in str(part) for part in finding)


def test_tracked_files_are_clean():
    assert scan_tracked() == []
