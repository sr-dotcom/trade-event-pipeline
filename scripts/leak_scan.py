"""Find secrets and identifiers in tracked files. Prints rule and place only, never the value.

Run: python -m scripts.leak_scan
"""
import re
import subprocess
import sys

# Brackets such as [K] keep this file from matching its own rules.
RULES = {
    "guid": r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    "shared_access_key": r"SharedAccess[K]ey=\S{4,}",
    "account_key": r"Account[K]ey=\S{4,}",
    "connection_string": r"Endpoint=sb:/{2}[\w.-]+",
    "databricks_url": r"[\w.-]+\.azuredatabricks\.net",  # the whole host, with the workspace number in it
    "databricks_workspace_id": r"workspace[I]d\W{1,4}\d{8,}",
    "account_puid": r"\bpu[i]d\W{1,4}[0-9a-f]{12,}",  # a Microsoft account ID in `createdBy` blocks
    "email": r"[\w.+-]+@[\w-]+\.[\w.-]+",
    "databricks_token": r"\bdapi[0-9a-f]{32}\b",
}
COMPILED = {name: re.compile(pattern, re.IGNORECASE) for name, pattern in RULES.items()}


def scan_text(text):
    """Return a list of (rule, line_number). Line numbers start at 1."""
    return [
        (name, number)
        for number, line in enumerate(text.splitlines(), start=1)
        for name, rule in COMPILED.items()
        if rule.search(line)
    ]


def scan_tracked():
    """Scan every file that git tracks. Return a list of (rule, path, line_number)."""
    names = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout.splitlines()
    findings = []
    for name in names:
        try:
            with open(name, encoding="utf-8") as handle:
                text = handle.read()
        except (UnicodeDecodeError, FileNotFoundError):
            continue  # binary file, or a file deleted in the working tree
        findings += [(rule, name, number) for rule, number in scan_text(text)]
    return findings


if __name__ == "__main__":
    results = scan_tracked()
    for rule, path, number in results:
        print(f"{rule}: {path}:{number}")
    sys.exit(1 if results else 0)
