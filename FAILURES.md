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

## 2026-11-05 Near miss: `run()` would have broken storage addresses (fixed before any Azure run)
- What: a read of `run_local.py` for the Azure plan found two faults. `run()` joined paths with `pathlib`, which turns `abfss://CONTAINER/x` into `abfss:/CONTAINER/x` (one slash lost). It also asked the local disk whether the checkpoint folder exists. Both work on a laptop and fail on storage. It could read only a path, so no run function could use the Kafka reader.
- Cause: the pipeline was built and tested on a laptop only. The tests used local folders.
- Fix: `run()` joins storage addresses as text and checks the checkpoint through Spark. It accepts a streaming DataFrame as input. Spark Connect (serverless) has no JVM handle, so the check does not use one.
- Test: `tests/test_run_uri.py` (4 tests). The storage address is tested with a `file://` address, not a real `abfss://` one. Red first, but the first red was an import error, so it is weak.

## 2026-11-05 A helper script used the wrong argument name and reported success (fixed)
- What: a helper script ran `az eventhubs eventhub authorization-rule keys list ... -n NAME`. That command takes `--authorization-rule-name`. Both connection strings came back empty. The secret was not saved, the sender crashed, and the script still printed "Done".
- Cause: `-n` works for `create` and `show`, not for `keys list`. A PowerShell script with `$ErrorActionPreference = 'Stop'` does not stop on a failed native command.
- Fix: the script uses `--authorization-rule-name` and checks `$LASTEXITCODE` after each command.
- Note: the script had no test. It ran once with real Azure.

## 2026-11-05 Probe notebook asked the generator for too few events (fixed)
- What: check P2 failed with `ValueError: Sample larger than population or is negative`.
- Cause: the notebook called `generate(n_events=200)`. The generator samples from fixed lists and needs more events. The same error appears on a laptop, so Azure was not the cause.
- Fix: the notebook asks for 2,000 events, the size the tests use.
- Test: `tests/test_probe_notebook.py` reads the number from the notebook and runs the generator with it. Red first, with the same error.

## 2026-11-05 A line break in the stored connection string broke the Kafka login (fixed)
- What: check P3 failed with `Failed to create new KafkaAdminClient`.
- Cause: the connection string was saved to a Databricks secret through a PowerShell pipe, which adds a line break. Inside the login setting the line break made it invalid. A check on the secret showed whitespace at the end, a length of 173, and a correct start, policy name and host.
- Fix: `kafka_options` strips whitespace from the connection string.
- Test: `tests/test_kafka_login.py`. Red first: the output showed the line break inside the setting. Earlier tests used clean strings.

## 2026-11-05 The Databricks workspace delete is slow and leaves a managed group (workaround)
- What: `az group delete` on the resource group took about 20 minutes, because the workspace stayed in `Deleting`. Afterwards the Databricks managed group was still there.
- Fix: a direct `az group delete` on the managed group removed it. The same happened in an earlier session.
- Note: the hub was deleted first, so the hourly meter stopped before the slow step.

## 2026-11-05 `compare.py` had the same path bug as `run()` (fixed before use)
- What: reading `scripts/compare.py` for the full Azure run showed that it joined the lake path with `pathlib`. It could not read an `abfss://` lake. It never failed in practice, because every run so far used a local lake.
- Fix: it uses the same join as `run()`.
- Test: `tests/test_run_uri.py`, with a `file://` lake. A real storage address is not tested.

## 2026-11-05 The upload to the workspace left out the `scripts` folder (fixed)
- What: the full-run notebook failed with `ModuleNotFoundError: No module named 'scripts'`. It failed again after the upload, because the notebook still held the old import state.
- Cause: the upload copied `src`, `config` and `notebooks`. The notebook imports `scripts.compare`.
- Fix: upload `scripts` too, then restart the Python process of the notebook.
- Note: no test checks that the upload list covers the notebook imports. This is still open.

## 2026-11-05 Unity Catalog objects outlived the deleted workspace (workaround)
- What: when the second workspace was created, the storage credential `sc-...` and the external location already existed from the first sitting.
- Cause: Unity Catalog objects live in the regional metastore, not in the workspace. Deleting the resource group does not remove them.
- Fix: the person deleted and recreated them. The teardown now deletes the external location and then the storage credential before the resource group.

## 2026-11-05 `awaitTermination()` never returned on serverless (fixed)
- What: the second Bronze run (R2) ran for more than 23 minutes and never ended. A second attempt hung the same way. The notebook printed "Listener thread still active, was not able to reconnect". Bronze already held all 50,000 rows and no stream was active.
- Cause: not found. On Spark Connect (serverless) the call to wait for the end of the query does not return, although the query is finished. A first run of the same code with 25,000 events had finished.
- Fix: `wait_for` in `src/pipeline/bronze.py` asks `query.isActive` every 2 seconds. It raises a clear error after 30 minutes, and re-raises an error of the query.
- Test: `tests/test_wait_for.py` (3 tests). Red first with an import error. On Azure the fix worked: R2 finished and passed.
- Limit: the test cannot show the serverless hang. Only the Azure run showed that the new wait ends.

## 2026-11-05 Serverless does not allow `avro` as a streaming source (fixed)
- What: the rebuild from the Capture archive (R5) failed with `UNSUPPORTED_STREAMING_SOURCE_PERMISSION_ENFORCED`. The Avro stream reader passed its tests on a laptop.
- Cause: this Databricks compute type does not allow the `avro` format as a streaming source.
- Fix: a rebuild starts from an empty lake and needs no checkpoint. `run()` accepts a batch DataFrame and writes Bronze in one batch (`batch_bronze`, mode overwrite). The notebook reads the archive with the batch reader.
- Test: `tests/test_run_batch.py`. Red first. It checks the counts and that a second run does not double Bronze.
- Note: the first fix attempt failed with `WRITE_STREAM_NOT_ALLOWED`, because the notebook still had the old module in memory. A restart of the Python process fixed it.

## 2026-11-05 The sitting ran 1 hour 52 minutes past its time limit
- What: the stop rule was 3 hours after the hub meter started (17:05). The hub was deleted at 21:57, after 4 hours 52 minutes. The cause was the two hung attempts of R2, and a wrong clock in my own messages: they gave guessed times, not times read from the computer.
- Fix: read the clock with a command before every time statement. Teardown first when a limit is crossed.
- Cost: not read from an invoice. The estimate for the hub alone is under 1 USD (UNVERIFIED).
