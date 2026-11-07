# Decisions

One entry per decision: date, choice, why.

## 2026-10-28 Azure region: francecentral
- Subscription: the Azure subscription of the project. Policy "Allowed resource deployment regions" allows only: westus, francecentral, mexicocentral, belgiumcentral, northcentralus.
- Choice: francecentral. Closest allowed region to Europe. Check Databricks, Event Hubs Standard and Azure SQL are offered there.

## 2026-10-28 Azure CLI sign-in
- The account is a guest in a tenant. WAM and device-code login failed (MFA security defaults; wrong tenant).
- Fix: set up MFA, then `az config set core.enable_broker_on_windows=false` and `az login --tenant <tenant id>` (browser flow).

## 2026-10-28 Budget alert
- Budget `project-40usd`, $40 monthly, on the project subscription. Alerts (actual cost) at 50, 75, 100 percent by email.
- Portal scope list did not show the subscription, so it was created with `az rest` (Microsoft.Consumption budgets API). A budget does not stop spending and lags by hours.

## 2026-10-31 Event Hubs Standard
- 2026-10-31: namespace created in francecentral (Standard, 1 TU, Kafka enabled). Deleted at end of session.

## 2026-10-31 Azure SQL free offer
- 2026-10-31: database `refdata` on a server in francecentral. Free offer applied (`useFreeLimit: true`), serverless GP_S_Gen5, auto-pause when the free limit ends, no overage billing, cost 0.
- SQL authentication. The admin password is kept outside the repo.

## 2026-10-31 Serverless compute
- Classic clusters are blocked on the student subscription (see FAILURES.md). Use serverless for Databricks jobs.
- Check that the resume proof works with micro-batch (`availableNow`) streaming. UNVERIFIED.

## 2026-10-31 Generator design
- Faults are added on top of clean trades. Duplicates, bad records and conflicts are extra records. The good events stay untouched. So the known answer is exact and the version chain of each trade is complete.
- Only delays change the good events: out of order (1 to 30 s), late (1 to 15 min), very late (1 to 6 h) and one 5-minute outage. The set of good events does not change.
- Total records = n_events exactly. The number of good events is n_events minus the extra records.
- Conflict rule (for the pipeline): same `event_id`, different content. The first arrival stays. The later record goes to quarantine with reason `event_id_conflict`. The generator makes the conflict arrive after the original.
- Bad records get a new `event_id` (`E-B...`). Reasons: `missing_field`, `bad_value`, `unknown_reference`.
- Arrival time: each record has `arrival_time_utc`. Locally it stands for `ingest_time`. On Azure, `ingest_time` comes from Event Hubs.
- Rates live in `config/faults.toml`. Profile A is the normal day. Profile B is the stress run (4 times more random faults). Default seed 42, 50,000 events.
- Output goes to `data/generated/<profile>/` (git-ignored): `events.ndjson`, `known_answer.json`, `expected_current_state.csv`. Pipeline tests compare to these.
- Reference data (`data/reference/`) is made up: 19 `FAKE0xx` instruments plus FAKEPACA, 30 counterparties, 10 books.

## 2026-10-31 Python worker fix on Windows
- Choice: a launcher script (`src/common/winspark/`), used by `tests/conftest.py`. Local tests only. No WSL2, no Databricks Connect.
- Why: smallest change, no new install, no cost. Details in FAILURES.md (F8).

## 2026-10-31 One shared Spark session in tests
- Choice: a session-scoped `spark` fixture with Delta in `tests/conftest.py`.
- Why: the JVM keeps the first session's settings. A second session without Delta would break the Delta tests.

## 2026-10-31 Bronze and Silver design
- Bronze keeps the raw JSON line as text (`value`). It adds `ingest_time`, `source` and `bronze_id`. Bad rows stay in Bronze. A rebuild can start from Bronze or from the archive.
- Silver reads every field as text first. This way a bad value (for example `quantity = "abc"`) is a row with a reason and not a read error.
- Reason order: `missing_field`, then `bad_value`, then `unknown_reference`. A conflict gets `event_id_conflict`.
- First arrival wins. Order is `ingest_time`, then `bronze_id`. A later row with the same `event_id` is a duplicate if the content is the same (SHA-256 of all other fields). It is a conflict if the content differs.
- Duplicates go to their own table, not to quarantine. A repeated delivery is normal and not an error. The count must still be visible.
- Late events stay in Silver with `is_late = true`. `is_late` means `ingest_time` is more than 300 seconds after `event_time_utc` (exactly 300 s is not late).
- Silver uses Spark built-ins only. No Python UDF. This also keeps the Windows worker problem (F8) away from the pipeline.
- The "read events" step is one small function (`read_events`). On Azure it reads Event Hubs (Kafka). Everything after it stays the same.
- Reference IDs go into `isin()` lists. This is fine for small lists. Use a join if the lists grow.
- Currencies are a reference file (`data/reference/currencies.csv`).
- The Windows set-up moved to `src/common/spark.py`, so the run script and the tests use the same code.
- Not done in this step: current-state table (highest `trade_version` per trade), Gold report, replay.

## 2026-10-31 Conflict rule: keep "first arrival wins"
- Question: a quantity can change before a deal is final. Should the latest copy win, and the first copy go to quarantine or duplicates?
- Answer: a real change is a new event (new `event_id`, higher `trade_version`, action AMEND). The highest version wins in the current-state table. The same `event_id` with different content is a contradiction, not a change.
- Choice: keep "first arrival wins". The later row goes to quarantine as `event_id_conflict`. A person decides.
- Why: it is not possible to know which row is true. "Last wins" lets a delayed old message overwrite a newer one without a sign. The known answer and the tests already use "first wins". A config switch adds code, tests and docs for 3 cases in 50,000 events.
- Not chosen: put the older row in `duplicates`. That table means exact copies only.
- Planned improvement: the quarantine row for a conflict also stores the `event_id` of the row that stayed in Silver and the names of the fields that differ. This gives the "show the difference" view of a conflict.
- Open: a config setting (first wins, last wins, hold both for review) may come after week 1, if it is needed.

## 2026-10-31 Current state
- The current state of a trade is the Silver event with the highest `trade_version`. Version order wins, not arrival order.
- A cancelled trade stays in the table. Column `is_active = false`.
- Proof: with `asc()` instead of `desc()` every trade looks active (34,380 instead of 32,311), and no error appears. The test against the known answer finds it.

## 2026-10-31 Gold report and versions
- A report has a cut-off (event time) and a publish time `as_of` (ingest time). It counts events that happened at or before the cut-off and that arrived by `as_of`. Cut-off is inclusive.
- The report shows, per book: active trades, total quantity and notional (quantity times price). The state of each trade is taken as of the cut-off, so an AMEND after the cut-off is not in the report.
- Version rule: the first publish is version 1. A later publish with different numbers is the next version. A later publish with the same numbers makes no new version. Old versions never change.
- Decision: keep old versions to show what a report showed at a given time.
- Local run: cut-off 15 minutes after the run start. Version 1 is published 5 minutes later. The second publish is at the end of the run. Real result (Profile A, seed 42): 2 versions. Late events that happened before the cut-off changed every book.
- Known limit: a report with no rows has nothing to store, so it gets no version.
- Test method: the real-data tests compare the report with an independent calculation in plain Python from the generator's records.

## 2026-10-31 Gold report per book and currency
- Change: the Gold report has one row per book and currency. It no longer adds amounts in different currencies.
- Why: the first version summed EUR, JPY, USD and others as one number. That number has no meaning. Per currency, a reader also sees where a problem is: in BOOK-01 the late events changed CHF, GBP and USD, and did not change EUR and JPY.
- Reason: a report must show where the problem is, so people can act on it.
- Not done: conversion to one currency. It needs exchange rates. There is no rate data, and fake rates would be a made-up number. Out of scope for this project.
- Real run (Profile A, seed 42): 100 Gold rows = 10 books x 5 currencies x 2 versions.

## 2026-10-31 Rebuild proof (replay)
- The archive is the raw file (`events.ndjson`). Locally it stands for the Event Hubs Capture archive on Azure. It is never changed.
- The proof: fingerprint all 6 tables, delete all 6, run the pipeline again from the raw file, fingerprint again. The two sets must be equal.
- Fingerprint: a SHA-256 hash for each row, sorted, then hashed again with the row count. It does not depend on row order. A null is not the same as the text "null".
- Why this can work: Silver breaks ties by `ingest_time` and then `bronze_id`. All sums use decimals. No step uses the current time or a random number.
- Real result (Profile A, seed 42, 50,000 events): all 6 tables are the same after the rebuild.
- Test: a changed archive (one raw event removed) gives a different Bronze fingerprint, so the check can fail.
- Known limit: the fingerprint collects all row hashes into one value. Fine for 50,000 rows. A different method is needed at millions of rows.
- Not done: the same proof on Azure (on Azure), with the Capture archive and Databricks.

## 2026-10-31 Conflict details in quarantine - correction
- Correction to the earlier entry "Conflict rule": The entry said the quarantine row would store the `event_id` of the row that stayed. That is wrong. Both rows have the same `event_id`, because that is what a conflict means.
- Built: a conflict row in quarantine has two more columns. `kept_bronze_id` is the Bronze id of the row that stayed in Silver. `differing_fields` lists the names of the fields that differ, in contract order (for example `quantity,price`).
- Other quarantine rows (missing field, bad value, unknown reference) have empty values in these columns.
- Real run (Profile A, seed 42): 3 conflicts, all differ in `quantity` only. The generator changes only the quantity.
- Not done: the values of the two rows side by side. A person can find the kept row with `kept_bronze_id` in Bronze.

## 2026-11-01 Azure scripts
- Choice: two PowerShell scripts in `infra/`. `setup.ps1` makes the resource group, ADLS Gen2, Event Hubs Standard (Kafka, Capture to ADLS) and the Databricks workspace. `teardown.ps1` runs `az group delete`. `teardown.ps1 -EventHubsOnly` deletes only the namespace (end of each session).
- Both scripts are a dry run by default. They print the commands. Only `-Go` runs them. This keeps "no billable resource without explicit approval" inside the script.
- The scripts never sign in and never print a key. Sign in by hand with `az login`. Capture uses the storage account in the same group.
- Tags on every resource that takes tags: project, owner, delete-by (7 days). `owner` is the generic word `student` (public repo).
- Names: storage and namespace get a 6-character hash of the subscription id, so they are unique and stable. No id is written in the repo.
- Tests (`tests/test_infra_scripts.py`) read the script text: tags, dry-run switch, Standard + Kafka + Capture, name rule, no secrets, teardown commands.

## 2026-11-01 Kafka read step
- Built: `src/pipeline/kafka_source.py`. `kafka_options` makes the Spark options for the Event Hubs Kafka endpoint (port 9093, SASL PLAIN, user `$ConnectionString`). `read_events_kafka` returns `value` (text) and `kafka_time`.
- The connection string is an argument. It is never in the repo.
- `to_bronze` uses `kafka_time` as `ingest_time` when that column exists. Without it, it uses `arrival_time_utc` as before. Local tests did not change.
- Not verified: the read itself (no Event Hubs on the laptop) and the `kafkashaded.` login module name on serverless. The `shaded` switch is there for this. Check on the next Azure session.
- Test lesson: `collect()` turns a Spark timestamp into the laptop's local zone (5 hours off). Compare times as UTC text inside Spark.

## 2026-11-01 Correction: ingest_time keeps the JSON arrival time
- Correction to "Kafka read step": The entry said `to_bronze` uses `kafka_time` as `ingest_time`. That is wrong for this project.
- Why wrong: the generator's late and out-of-order faults live in `arrival_time_utc` in the JSON. On Azure the Kafka time is the send time, and all 50,000 events are sent within minutes. With the Kafka time every event would look the same age, and the comparison with the known answer would fail. The first tests did not catch it, because they checked the rule I had written, not the known answer.
- Fix: `ingest_time` = `arrival_time_utc` from the JSON. If the JSON has none, use `kafka_time`. `kafka_time` stays as an extra column for information.
- Real-time sources (Alpaca, later): there the JSON has no arrival time, so the Kafka time is used.

## 2026-11-02 Repo hygiene
- Leak scan: `scripts/leak_scan.py` checks tracked files for GUIDs, key strings, connection strings, workspace URLs, email addresses and Databricks tokens. It prints the rule and the place, never the value. Test-first: `tests/test_leak_scan.py`. Two older tests held fake keys. We split those strings, so the scan stays strict.
- CI: `.github/workflows/ci.yml` runs the leak scan, ruff and pytest on Linux. It holds no secrets and never calls Azure. A test enforces this. It ran on GitHub (Ubuntu) on the first four pushes and passed each time (checked 2026-11-06). The Windows-only code paths are guarded by `sys.platform`.
- README: a claims table with PASS, NOT BUILT or UNVERIFIED for each claim. The expected output of the quick start is not checked on a clean machine yet.
- Deferred: a Markdown link check.

## 2026-11-02 Stable bronze_id
- Problem: `bronze_id` came from `monotonically_increasing_id()`. It changes with the Spark partitioning, and it restarts in each micro-batch. A rebuild would show Bronze and quarantine as different for the wrong reason.
- Choice: `bronze_id` = `xxhash64(value)`, a hash of the raw JSON text. An early idea was the partition and offset. The hash is preferred: a Kafka offset and a Capture offset may differ (UNVERIFIED), and the text does not.
- Tests first (`tests/test_bronze_id.py`): 3 of 5 failed on the old code (partitioning, two batches, file order). All 5 pass now. Full suite: 105 passed.
- Effect on the tie-break: two lines with the same `event_id` and the same `ingest_time` but different content now have a fixed winner (the lower hash). Before, the file order decided. The winner is arbitrary but the same on every run. The rule "first arrival wins" is unchanged when the arrival times differ.
- Identical lines share an id. This is harmless: Silver keeps one and counts the other as a duplicate (tested).
- Limits: xxhash64 is not a cryptographic hash. A collision of two different lines is possible but very unlikely at 50,000 events. `kafka_time` does not exist in the Capture archive, so the project does not claim that Bronze is equal after a rebuild.

## 2026-11-02 Streaming Bronze with a checkpoint (local, folder source)
- Built: `read_events_stream` (a folder of NDJSON files stands in for Event Hubs) and `stream_bronze` (append to the Bronze Delta table, `availableNow`, then stop). It is not a continuous stream.
- Rule: Bronze reads new events only. Silver, current state and Gold recompute from all of Bronze. Reason: dedup, first arrival wins and version order need every row, not one batch.
- Tests (`tests/test_bronze_stream.py`, 5 green): a second run adds no old events. Incremental Bronze equals one-shot Bronze (fingerprint). Silver, quarantine and duplicates from incremental Bronze equal the one-shot result, with a duplicate, a conflict and a bad record across two files. `maxFilesPerTrigger=1` gives 2 micro-batches and the same table. A lost checkpoint reads everything again (6 rows).
- Rule from the last test: a new checkpoint always goes with an empty Bronze table.
- The tests were red only because the functions did not exist. They were not red for a behavior reason.
- Not done: wiring into `run_local`, the Kafka stream version, the crash-switch test. UNVERIFIED: `availableNow` and the checkpoint on serverless with ADLS.

## 2026-11-02 Capture Avro reader (local fixture)
- Built: `src/pipeline/capture_source.py`, `read_events_capture`. It reads the Avro files that Event Hubs Capture writes and returns the column `value` (the `Body` as text). Same output as `read_events`.
- Red first: "Failed to find data source: avro". Local Spark had no Avro format. Fix: `get_spark` loads `org.apache.spark:spark-avro_2.13:4.1.1`. Databricks has Avro built in, so this change is local only.
- Tests (`tests/test_capture_source.py`, 3 green): the reader returns the sent lines. Bronze from the archive equals Bronze from the file (same value, ingest_time, source and bronze_id). Silver, quarantine and duplicates are equal, with a duplicate, a conflict and a bad record.
- Why Bronze can be equal: `ingest_time` comes from the JSON and `bronze_id` from the text. The archive has no `kafka_time`, and the reader does not add it.
- UNVERIFIED: the fixture follows the Azure docs from general knowledge (columns and folder layout). A real Capture file is read in the Event Hubs test. Whether the Capture `Offset` equals the Kafka offset is also UNVERIFIED.

## 2026-11-02 Independent compare script
- Problem: `run_local` computes `events_lost` from its own Bronze count. An event lost before Bronze is invisible to it. A test showed it: one line dropped from the file, `events_lost` stays 0.
- Built: `scripts/compare.py`. It reads `known_answer.json` and `expected_current_state.csv` (written by the generator before the run) and the Delta tables. It checks 10 things: events in, Silver, quarantine, quarantine by reason, duplicates, reconciliation (Silver + quarantine + duplicates = events in the known answer), late flagged, trades, active trades, and the version, action and quantity of every trade in the current state. Any difference gives exit code 1.
- Tests (`tests/test_compare.py`, 5 green): a clean run passes 10 of 10. A loss before Bronze fails `events_in` and `reconciliation`. A wrong known number fails only that check. A wrong expected trade version fails the current-state check. The exit codes are 0 and 1.
- Real run: 10 of 10 PASS on the 50,000-event Profile A lake (48,630 Silver, 920 quarantine, 450 duplicates, 843 late, 34,380 trades, 32,311 active).
- Limits: the Gold report is not compared (its own test uses an independent calculation). The known answer comes from the same generator code that made the events. It is independent of the pipeline, not of the generator. Fingerprint equality of a rebuild stays in `replay`.

## 2026-11-02 Gold is append-only across runs
- Problem: `run_local` started each run from an empty Gold table and overwrote it. A changed input could silently rewrite a published version.
- Test first (`tests/test_gold_append_only.py`, 2 tests): run 1 publishes version 1 (quantity 100). Run 2 adds an AMEND that arrived before the 14:20 publish time (quantity 250). The first test failed on the old code: the version 1 row from run 1 was gone. The second test (same input again adds nothing) passed on the old code and on the new code.
- Fix: `run_local` reads the published Gold table if it exists, publishes on top of it, and appends only the new version rows (`mode("append")`). The other tables are still overwritten. They are rebuilt from Bronze.
- Rule: a published version never changes. A different number for the same cut-off becomes the next version. The published_at of such a version is the publish time of the run's schedule, not the wall-clock time of the run. This is a simplification.
- The new rows are collected first (a few rows), because the append writes to the table that the published rows came from. This also avoids `cache()`, which serverless does not support.
- `replay` still works: it deletes Gold on purpose and rebuilds it. A rebuild gives the same Gold fingerprint (tested).
- Limits: only the local run does this. The Azure notebooks do not exist yet. Delta also keeps its own table history, but the project does not rely on it for the audit trail. UNVERIFIED: `DeltaTable.isDeltaTable` and the append on ADLS with serverless.

## 2026-11-02 run_local reads Bronze through the stream
- Change: `run_local` now calls `stream_bronze` (checkpoint in `<lake>/_checkpoint/bronze`) and builds Silver, quarantine, duplicates, current state and Gold from all of the Bronze table. Bronze is no longer overwritten. This is rule D1.
- Finding 1: a file stream needs a folder, not one file ("Option basePath must be a directory"). Fix: for one file, `read_events_stream` reads the parent folder with `pathGlobFilter` set to the file name. Folders and single files both work.
- Finding 2 (the trap): a Bronze table with no checkpoint, for example the lake from the old code, would be read again and doubled. `run` now raises an error with a clear message. Test: `test_run_refuses_a_bronze_table_that_has_no_checkpoint`. `replay` deletes the checkpoint with the tables, because a new Bronze needs a new checkpoint.
- Finding 3 (a bug in the previous step): a second run on the same events published new Gold versions 3 and 4. Gold publishes at 14:20 first, then at the end of the data. On a rerun the 14:20 report differs from the latest stored version, so the old numbers were published again as a new version. The earlier append-only test missed it: its data had the same numbers at 14:20 and at the end. The new streamed-run test found it.
- Fix 3: `publish_report` makes no new version when the publish time is earlier than the latest version. Versions go forward in time. Unit test: `test_a_publish_time_earlier_than_the_latest_version_makes_no_new_version` (red first).
- Tests changed: the Gold append-only helper now drops new events as new files in a landing folder. A file that is rewritten in place is not read again by a stream. New events must arrive as new files.
- Limits: the local run only. A file overwritten in place is not re-read. UNVERIFIED: the same on Azure with the Kafka source, where the offsets play the role of the files.

## 2026-11-02 Redaction function
- Why: The Azure run makes logs and portal output. They carry subscription IDs, tenant IDs, emails, workspace URLs, sometimes keys, and local paths. The evidence folder must hold none of them.
- Built: `scripts/redact.py`. It reuses the leak-scan rules and replaces each match with a label (`<guid>`, `<email>` and so on). It reads the account name, computer name and home path from the environment and replaces them with `<local>`, in any case and with both slash styles. Short or empty values are skipped, so "a" is not replaced everywhere.
- Tests (`tests/test_redact.py`, 13 green, red first): each planted fake is removed and the leak scan then finds nothing. The layout of the text stays. A resource listing keeps the resource group name and loses the subscription ID. A connection string loses its key value. Local strings come from the environment. Clean text is unchanged, and a second pass changes nothing. The command reads a file and writes a clean file.
- Checked on this machine: a sample with the real user name, computer name and home path came out with labels only. The leak scan found nothing in the result.
- Limits: it removes known patterns only. Read every clean file before it is committed. A name that is not in the rules, for example a person's name in free text, is not found. The full suite was not re-run for this step: no source file changed, only a new script and a new test file.

## 2026-11-02 Kafka stream version
- Built: `read_events_kafka(..., stream=True)` uses `spark.readStream` with the same options. The default is still the batch reader. A new function `kafka_events` turns the Kafka source columns into the events columns: `value` (text), `kafka_time`, `kafka_partition`, `kafka_offset`.
- New columns: the partition and the offset go into Bronze (`to_bronze` passes `kafka_time`, `kafka_partition` and `kafka_offset` through when they exist). Reason: later, compare them with the Capture `Offset`. The key of a Bronze row is still the hash of the text (see the stable bronze_id entry). The offset is provenance, not the key.
- Tests (`tests/test_kafka_stream.py`, 4 green): the stream flag picks `readStream` and passes the same options to the `kafka` format. The default stays `read`. `kafka_events` returns the four columns with the text value. A folder of files that has the Kafka source columns goes through `stream_bronze` with a checkpoint: a second run adds nothing, a new file adds only its events, the offsets 0, 1, 2 arrive in Bronze.
- The tests were red only because `kafka_events` did not exist (weak red). The fake Spark object records which reader is used. It is a stand-in, not Kafka.
- Not done and UNVERIFIED until an Azure Event Hubs test: a real read from Event Hubs, the `kafkashaded.` login module on serverless, `maxOffsetsPerTrigger`, whether the Capture `Offset` equals the Kafka offset, and the checkpoint on ADLS.

## 2026-11-03 Crash test (failed micro-batch)
- Built: `tests/test_bronze_crash.py`. No change to pipeline code. A poison event in file 3 makes a function raise while a flag file exists. Bronze reads one file per micro-batch.
- Result: the first run fails in batch 3 and Bronze holds 6 rows. After the flag is deleted, a run with the same checkpoint gives 9 rows, 9 different `bronze_id` values, and a fingerprint equal to a clean one-shot read.
- The test passed at once. It checks the claim about Spark and the checkpoint. It is not red first.
- Not tested: a crash inside a Delta commit, a killed process, and a crash on serverless. The README says "A failed micro-batch is redone. A crash inside a commit was not tested."

## 2026-11-04 The pipeline stops reading the generator config
- Problem: `run_local.run` called `load_config("A")` from the generator. The pipeline took its report start date from the generator. Events from another day gave an empty Gold report, with no error. Shown on 2030 events: 0 Gold rows.
- Built: `config/pipeline.toml` holds `start_utc`, `cutoff_minutes` and `first_publish_minutes`. `load_report_config()` reads it. `run(..., report=None)` takes the settings as an argument and falls back to the file. No file in `src/pipeline` imports `src.generator` now.
- Cost: the start date now lives in two files, `faults.toml` (generator) and `pipeline.toml` (pipeline). A test fails if they differ. These are report settings, not fault rates, so the rule "fault rates in one file" still holds.
- Tests (`tests/test_pipeline_config.py`, 3 green): no generator import in `src/pipeline`. The report follows the settings it is given. The two start dates are equal.
- The tests were red only because `load_report_config` did not exist (weak red). The 2030 demo before the change showed the real behavior.

## 2026-11-04 Setup switch: setup.ps1 -NoEventHubs
- Why: the first Azure step needs the storage account and the workspace only. Event Hubs bills by the hour and has no use yet.
- Built: a `-NoEventHubs` switch in `infra/setup.ps1`. It skips the namespace and the hub. The final messages change with it.
- Found while testing: a script with a `param()` block and no `[CmdletBinding()]` ignores unknown switches. A typo would have created Event Hubs and started billing. `[CmdletBinding()]` makes an unknown switch stop the script. Note: PowerShell accepts short forms of a switch, so `-NoEventHub` is the same as `-NoEventHubs`.
- Tests (`tests/test_infra_scripts.py`, 3 new, 11 green): they run the dry run with a fake `az` function. They check the default list, the switch, and the unknown switch. Nothing is created. The first two were red for a real reason (the switch was ignored).

## 2026-11-04 Subscription guard in setup.ps1
- Decision: `setup.ps1` checks the default subscription by name and stops if it is not "Azure for Students". See FAILURES.md, same date.
- Why a name and not an ID: the ID is an identifier that stays out of the repo. The name is not secret and it is readable.
- Limit: it checks the name only. Two subscriptions with the same name would pass.

## 2026-11-05 Match Capture rows to Kafka rows by SequenceNumber
- Decision: a rebuild from the Capture archive uses `SequenceNumber` when it needs a position. It does not use `Offset`.
- Why: on 100 real events, `SequenceNumber` equals the Kafka offset for every event, and `Offset` equals it for none (see `docs/azure-probe-results.md`).
- Limit: one partition and 100 events. Bronze still keys rows by the hash of the text, so a rebuild does not depend on this.

## 2026-11-05 Access for the Azure probe
- Decision: two policies on the event hub, one that can only send and one that can only listen. The root key is not used. Storage access goes through a managed identity with the role Storage Blob Data Contributor, and the person who sets up Capture needs Storage Blob Data Owner.
- Decision: the external location was created without file events. Their set-up needs three more roles, among them Storage Account Contributor. The project does not use file events, so the identity keeps the one role it needs.
- Limit: no denied-access test was run. "Governed" is not claimed, only "access works".

## 2026-11-05 The Azure rebuild builds a new lake and compares it
- Decision: the rebuild on Azure does not delete tables. It builds a second lake from the Capture archive, then compares the two lakes with `lake_fingerprints`.
- Why: deleting tables on storage is harder to undo than writing to a new folder. The first lake stays as it is, so a failed rebuild costs nothing.
- Rule for the comparison: Bronze is compared on the columns every source has (`value`, `ingest_time`, `source`, `bronze_id`), because only Kafka rows carry `kafka_time`, `kafka_partition` and `kafka_offset`. Gold is compared on its latest version without the version number. A lake built in several runs keeps older Gold versions. A lake built in one run does not.
- Tests: `tests/test_lake_compare.py` builds a lake from a file and a lake from a Capture-format fixture. All six comparisons are equal.
- Limit: tested on a local fixture of 2,000 events. A real archive is not tested.

## 2026-11-05 The Capture rebuild reads the archive in one batch
- Decision: `run()` accepts a batch DataFrame and writes Bronze with `batch_bronze` (overwrite, no checkpoint). A stream input keeps the checkpoint and the guard against a Bronze table without a checkpoint.
- Why: serverless does not allow `avro` as a streaming source. A rebuild reads the whole archive once into a new lake, so a checkpoint has no use. Overwrite makes a second rebuild give the same Bronze, not a doubled one.
- Result: 6 of 6 tables equal the Kafka lake on 50,000 real events (see `docs/azure-run-results.md`).
- Limit: the streaming Capture reader stays in the code and in its local test. It cannot run on serverless. Incremental reading of new archive files is not shown on Azure.

## 2026-11-05 Wait for a stream by asking `isActive`
- Decision: `stream_bronze` waits with a loop over `query.isActive`, 2 seconds apart, with a limit of 1,800 seconds.
- Why: `awaitTermination()` did not return on serverless. The loop works in both places.
- Limit: a stream that really runs longer than 30 minutes needs a larger limit.

## 2026-11-06 A lake built in several runs can hold an intermediate Gold version
- Decision: no change to the code. Each run publishes at the first publish time and at the last ingest time. A run on part of the events can publish a version that a later run on all events then replaces as the latest.
- Why: Gold is append-only. A published version is never rewritten, even if later events show it was incomplete. That is the rule for reports that people may already have read.
- Evidence: the 50,000 events on Azure came in two halves and gave 3 versions. The same split on a laptop gave the same 3 versions. The latest version equals the single run in both cases.
- Test: `tests/test_gold_split_run.py` (characterization test, green at the first run).
- Limit: the count of versions depends on how the events are split into runs. Only the latest version is compared with the known answer.
