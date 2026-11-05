# Failures

Every real failure: date, what happened, cause, fix.

## 2026-10-28 pyspark 4.1.4 conflicts with delta-spark 4.2.0 (fixed)
- What: `pip install pyspark==4.1.4 delta-spark==4.2.0` failed with `ResolutionImpossible`.
- Cause: delta-spark 4.2.0 requires `pyspark<=4.1.1,>=4.0.1`. The pin 4.1.4 was an assumption.
- Fix: pin pyspark 4.1.1. It is on the same 4.1 line as Databricks Runtime 18 LTS (Spark 4.1.0).

## 2026-10-28 Local Delta write fails on Windows (fixed)
- What: `tests/test_t1_delta.py` failed with `FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`.
- Cause: Hadoop on Windows needs `winutils.exe` and `hadoop.dll`. Neither was present.
- Fix: put matching `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`. The files came from a Hadoop 3.4.0 build of the third-party project `kontext-tech/winutils` (folder `hadoop-3.4.0-win10-x64`). PySpark bundles Hadoop 3.4.2. These are third-party binaries: check the source before you use them.
- Test: `tests/test_t1_delta.py` passes (Delta write and read from SQL-built data).

## 2026-10-31 `hadoop.dll` not found on PATH (fixed)
- What: `tests/test_t1_delta.py` failed on read with `UnsatisfiedLinkError NativeIO$Windows.access0`.
- Cause: `C:\hadoop\bin` was not on `PATH`. Java loads `hadoop.dll` from `PATH`. `HADOOP_HOME` alone is not enough. An earlier pass had run in a shell that had it.
- Fix: `src/common/spark.py` adds `%HADOOP_HOME%\bin` to `PATH` before the first Spark session. No system setting changes.

## 2026-10-28 Python worker crashes on Windows (workaround)
- What: `spark.createDataFrame([...])` from Python rows failed with "Python worker exited unexpectedly (crashed)" and an `EOFException`. No Python traceback appeared.
- Tried without success: `PYSPARK_PYTHON` set to the venv Python, `spark.python.use.daemon=false`, `worker.reuse=false`, Arrow off. The worker module starts fine when run by hand.
- Cause: `python -m pyspark.worker`, the way Spark starts a worker, dies without a Python error. The exit code is 0. The working folder, the core count and an exit delay are not the cause (all tested).
- Fix: Spark starts a launcher, `src/common/winspark/python.cmd`, as `PYSPARK_PYTHON`. For `pyspark.worker` it runs `win_worker.py`. That file imports `pyspark.worker` and calls `main`. `src/common/spark.py` sets this up on Windows.
- Open: a no-op wrapper around `handle_worker_exception` in `win_worker.py` is needed. Without it the crash comes back. The reason is not known. Treat it as a workaround.
- Test: `tests/test_pyspark_python_worker.py` (`createDataFrame` and a Python UDF) passes. Two full runs passed.

## 2026-10-31 No usable VM for classic Databricks clusters on the student subscription (workaround: serverless)
- What: an Azure Databricks workspace (premium, `francecentral`) was created. It opens with Entra sign-in. Runtime 18 LTS (Spark 4.1.0) is offered. A single-node classic cluster failed through the API with `CLOUD_PROVIDER_RESOURCE_STOCKOUT / SkuNotAvailable` for `Standard_D4ds_v4`.
- Findings (`az vm list-usage`, `az vm list-skus`) for an Azure for Students subscription:
  - The regional vCPU limit is 6 in each allowed region. The per-family limit is 4, but only for old families (v3, v4, Dv2).
  - The v5 and v6 families have a limit of 0. All `francecentral` sizes are restricted.
  - In `westus` and `northcentralus`, the v3 and v4 sizes are restricted. Only restricted-use or GPU sizes remain.
  - `belgiumcentral` has `D4ds_v4` and quota, but Azure Databricks does not run there.
  - `mexicocentral` has no matching quota.
- Cause: capacity restrictions on the student offer. The quota cannot be raised.
- Workaround: serverless compute needs no VM and no core quota. A serverless notebook ran `spark.range(5).count()` and returned 5 in 2.5 s.
- Trade-off: serverless runs streams as micro-batches (`availableNow`), not as a continuous stream. That the pipeline's checkpoint and resume logic works on serverless is UNVERIFIED.

## 2026-10-31 Local run failed with `DELTA_METADATA_MISMATCH` (fixed)
- What: after the Gold report got a `currency` column, `python -m src.pipeline.run_local` stopped with `DELTA_METADATA_MISMATCH` on the old `data/lake/gold_report` table.
- Cause: Delta does not let `mode("overwrite")` change the columns of an existing table. The old table had no `currency` column. The tests did not find it, because each test writes into a new folder.
- Fix: the run script writes with `option("overwriteSchema", "true")`. The local run rebuilds most tables each time, so a new schema is fine there.
- Note: on Azure, a live table must not be overwritten like this. A schema change there needs a planned step.

## 2026-11-04 Near miss: the CLI default subscription was the wrong one (fixed)
- What: the Azure CLI default subscription was not the one that holds the budget alert. `setup.ps1` used the default and would have created the resource group there.
- Cause: `setup.ps1` read the subscription ID from `az account show` and never checked the name. The CLI default is a local setting that can change after a new sign-in.
- Fix: `setup.ps1` has a `-Subscription` parameter (default "Azure for Students"). It stops, in a dry run too, if the default has another name. The message gives the command `az account set --subscription ...`.
- Test: `test_setup_refuses_to_run_in_the_wrong_subscription` (fake `az`, red first, green after). Checked against the real CLI: the dry run stops with the message and creates nothing.

## 2026-11-05 Redaction missed three identifiers in real `az` output (fixed)
- What: the first real `setup.ps1 -Go` output went through `scripts/redact.py`. The clean file still held the Databricks workspace number (in `workspaceUrl` and in a `workspaceId` line) and a Microsoft account ID (`puid`). The unit tests had passed.
- Cause: the old URL rule matched only the last part of the host, so `adb-<number>.` stayed. No rule covered `workspaceId` or `puid`. The fakes in the tests had the shapes we expected, not the shapes Azure prints.
- Fix: the URL rule matches the whole host. Two new rules: `databricks_workspace_id` and `account_puid`. Both have a fake in `tests/test_leak_scan.py`, so the leak scan and the redact tests cover them. The tests were red first.
- Caught by: reading the clean file by hand before it was committed.
- Note: more shapes may exist. The rules find known patterns only.

## 2026-11-05 `teardown.ps1` ignored a mistyped switch (fixed)
- What: a review of the README found that `.\infra\teardown.ps1 -EventHubOnly -Go` (missing the `s`) ran without an error. The script ignored the unknown switch and deleted the whole resource group instead of only Event Hubs.
- Cause: a script with a `param()` block and no `[CmdletBinding()]` accepts unknown switches. `setup.ps1` had been fixed for this earlier. `teardown.ps1` had not.
- Fix: `teardown.ps1` has `[CmdletBinding()]`. An unknown switch stops the script.
- Test: `test_a_mistyped_teardown_switch_stops_instead_of_deleting_the_whole_group` (fake `az`, red first, green after).

## 2026-11-05 `redact.py` skipped UTF-16 files (fixed)
- What: a review found that Windows PowerShell 5.1 writes UTF-16 when you redirect output with `>`. The tool read every file as UTF-8, so a UTF-16 file was not cleaned, and no warning appeared.
- Fix: the tool detects a UTF-16 byte order mark and decodes the file as UTF-16.
- Test: `test_a_utf16_file_from_windows_powershell_is_cleaned_too` (red first, green after).
