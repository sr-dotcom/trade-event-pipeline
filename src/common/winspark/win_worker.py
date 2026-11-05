"""Python worker launcher for Windows (see FAILURES.md: "Python worker crashes on Windows"). Local tests only.

`python -m pyspark.worker` crashes on Windows. This file runs the same worker code
as a script, and it works. The no-op wrapper around handle_worker_exception is needed:
the run crashes without it. The reason is not known.
"""

import os

import pyspark.worker as w

orig = w.handle_worker_exception


def patched(e, outfile):
    return orig(e, outfile)


w.handle_worker_exception = patched
conn = int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", "-1"))
sock_file, _ = w.local_connect_and_auth(conn, os.environ.get("PYTHON_WORKER_FACTORY_SECRET"))
w.write_int(os.getpid(), sock_file)
sock_file.flush()
w.main(sock_file, sock_file)
