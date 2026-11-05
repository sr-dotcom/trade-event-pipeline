"""Clean run output before we commit it as evidence.

Each match of a leak-scan rule becomes a label such as <guid>. Local strings from the environment
(user name, computer name, home path) become <local>. Names of resources, counts and timings stay.
After redaction the leak scan finds nothing in the text.

Run: python -m scripts.redact run.txt run.clean.txt
"""

import argparse
import codecs
import os
import re

from scripts.leak_scan import COMPILED

# Environment variables that name this machine or this person. A value shorter than 3 characters is skipped.
LOCAL_KEYS = ("USERNAME", "USER", "LOGNAME", "COMPUTERNAME", "HOSTNAME")
HOME_KEYS = ("USERPROFILE", "HOME")  # paths: we also remove the other slash style


def local_strings(env=None):
    """The exact strings that identify this machine, longest first."""
    env = os.environ if env is None else env
    found = {env.get(key, "").strip() for key in LOCAL_KEYS}
    for key in HOME_KEYS:
        home = env.get(key, "").strip()
        found |= {home, home.replace("\\", "/"), home.replace("/", "\\")}
    return sorted((s for s in found if len(s) >= 3), key=len, reverse=True)


def redact_text(text, local=()):
    for name, rule in COMPILED.items():
        text = rule.sub(f"<{name}>", text)
    for string in local:
        text = re.sub(re.escape(string), "<local>", text, flags=re.IGNORECASE)
    return text


def main(argv=None, env=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("source", help="the raw output file")
    p.add_argument("target", help="where to write the clean file")
    a = p.parse_args(argv)
    with open(a.source, "rb") as f:
        raw = f.read()
    # Windows PowerShell 5.1 writes UTF-16 with a byte order mark when you redirect with `>`.
    utf16 = raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE))
    text = raw.decode("utf-16" if utf16 else "utf-8", errors="replace")
    with open(a.target, "w", encoding="utf-8", newline="") as f:
        f.write(redact_text(text, local_strings(env)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
