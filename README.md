# trade-event-pipeline

A trade-event pipeline built with PySpark and Delta Lake, planned for Azure Databricks. It reads fake trade events, sorts good records from bad ones, and builds versioned reports. A separate script checks the result against an answer that the data generator wrote before the pipeline ran.

> **Status: work in progress.**
> The pipeline runs on a laptop and its tests pass.
> A small probe ran on Azure: 100 events, four checks, all passed. See [the probe results](docs/azure-probe-results.md).
> The full run then sent 50,000 events through Event Hubs to Databricks serverless. The tables equal the known answer, and a lake rebuilt from the Capture archive equals the first lake. See [the run results](docs/azure-run-results.md). The Azure resources were deleted afterwards.
> The [claims table](#claims-and-evidence) says what is proven (PASS) and what is not (UNVERIFIED).

## Limits

- The data is fake. A generator makes it. The project uses no real market data and no client data.
- This is a learning project. It is not production code. It makes no claim about regulation or compliance.
- The fault rates are assumptions, not measurements. They are in `config/faults.toml`.
- The pipeline reads events in micro-batches. It is not a continuous stream.
- The 50,000-event run and the comparison use seed 42 and profile A. The tests also use seed 7 and profile B (20,000 events). Everything ran on one machine (Windows 11).

## Words used in this guide

| Word | Meaning |
|---|---|
| Bronze, Silver, Gold | Three table layers: raw, cleaned, and reports. |
| Quarantine | The table for bad records. Each row has a reason. |
| Duplicates | The table for repeated deliveries of the same event. |
| Known answer | Files that the generator writes. They say how many records each table must hold, and the expected state of each trade. |
| Profile | A set of fault rates in `config/faults.toml`. Profile A is a normal day. Profile B has about four times more random faults. |
| Seed | A number that makes the generator repeatable. The same seed gives the same events. |
| Book | A trading book: a group of trades. The Gold report has one row per book and currency. |
| Counterparty | The other party in a trade. |
| Notional | Quantity times price. |
| Cut-off | The event time up to which a Gold report counts trades. |
| Append-only | Rows are added and never changed. Gold keeps every published version. |
| Checkpoint | A saved reading position. A run that stops can continue from it. |
| Micro-batch | A read of the events that arrived since the last run. Then the job stops. In Spark this is the `availableNow` trigger. |
| Rebuild | Delete the tables and build them again from the raw file. The command is called `replay`. |
| Fingerprint | A hash of the content of a table. Two tables with the same fingerprint hold the same rows. |
| Dry run | A run that prints what it would do and changes nothing. |
| Kafka | A message log. A producer adds messages at the end. A consumer reads from a position. |
| Event Hubs | The Azure service that accepts the Kafka protocol. |
| Capture | An Event Hubs feature that copies every event to files in storage. |
| ADLS | Azure Data Lake Storage Gen2. A storage account with a folder structure. |
| Avro | The file format that Capture writes. |
| Delta | A table format on top of Parquet files. It keeps a transaction log. |
| Serverless | Databricks compute that you do not size or start yourself. |
| NDJSON | A text file with one JSON object on each line. |
| Temurin | A free build of Java. |
| Lint | An automatic check of code style. |
| PASS, UNVERIFIED | Status words. PASS means a test or a run shows the claim. UNVERIFIED means the code or the plan exists and no run shows the claim. |

## Contents

- [Tutorial: run the pipeline on your laptop](#tutorial-run-the-pipeline-on-your-laptop)
- [How-to guides](#how-to-guides)
- [Reference](#reference)
- [Why it works this way](#why-it-works-this-way)
- [Claims and evidence](#claims-and-evidence)
- [What is next](#what-is-next)

The [architecture page](docs/architecture.md) has a diagram of the data flow.

## Tutorial: run the pipeline on your laptop

You will make 50,000 fake events, run the pipeline, and check the result against the known answer. This takes about 10 minutes the first time.

### Before you start

You need:

- Git.
- Python 3.12.
- Java 21. The tests use Temurin 21.
- An internet connection. The first Spark run downloads the Delta and Avro libraries from Maven Central.
- On Windows only: `winutils.exe` and `hadoop.dll`. See [Set up Hadoop files on Windows](#set-up-hadoop-files-on-windows).

This project was built and tested on Windows 11. The file `.github/workflows/ci.yml` runs the leak scan, ruff and the tests on Ubuntu. It passed on GitHub on each of the first four pushes (checked 2026-11-06). The 50,000-event run and the comparison were done on Windows only. macOS is UNVERIFIED.

#### Set up Hadoop files on Windows

Spark on Windows needs two Hadoop files. See the [FAILURES.md](FAILURES.md) entry "Local Delta write fails on Windows". The files are third-party binaries, so check their source before you use them.

1. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.4. The FAILURES.md entry names the build that this project used.
2. Put both files in `C:\hadoop\bin`.
3. Set the environment variable `HADOOP_HOME` to `C:\hadoop`, for your account.
4. Open a new terminal, so that it sees the variable.

### Steps

1. Get the repository with `git clone`, and open a terminal in its folder. A ZIP download works for the tutorial, but one test needs Git.
2. Check your versions.

   ```text
   python --version
   java -version
   ```

   The first command prints `Python 3.12.` and a patch number. The second prints a line that contains `version "21`.

3. Create a virtual environment.

   **Windows (PowerShell):**

   ```powershell
   python -m venv .venv
   ```

   **macOS or Linux:**

   ```bash
   python3 -m venv .venv
   ```

4. Install the packages.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m pip install -r requirements.txt
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m pip install -r requirements.txt
   ```

5. Make the fake events.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m src.generator.generate --profile A --n 50000 --seed 42
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m src.generator.generate --profile A --n 50000 --seed 42
   ```

   The output ends with a list of what the generator injected and what the right result must be. The first line shows the full path of `data/generated/A` on your machine.

   ```text
   Wrote 50000 events to <full path of data/generated/A>

   injected:
     duplicate_random             250
     duplicate_burst              200
     out_of_order                 1000
     late                         500
     very_late                    50
     bad_random                   250
     bad_window                   667
     conflict                     3
     outage_events                8156

   expected:
     good_unique_events           48630
     duplicates                   450
     quarantine                   920
     quarantine_by_reason         {'missing_field': 346, 'bad_value': 323, 'unknown_reference': 248, 'event_id_conflict': 3}
     late_flagged                 843
     trades                       34380
     cancelled_trades             2069
     active_trades                32311
     version_inversion_trades     463
   ```

6. Run the pipeline. It reads `data/generated/A/events.ndjson` and writes Delta tables to `data/lake`.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m src.pipeline.run_local
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m src.pipeline.run_local
   ```

   Spark prints a lot of log text first. The last lines show the counts. They match the `expected` block above:

   ```text
   events_in      50000
   silver         48630
   quarantine     920
   duplicates     450
   late_flagged   843
   trades         34380
   active_trades  32311
   gold_versions  2
   events_lost    0
   ```

   `gold_versions` is the number of Gold report versions. `events_lost` is `events_in` minus Silver, quarantine and duplicates. It must be 0.

7. Compare the tables with the known answer.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m scripts.compare --known data/generated/A --lake data/lake
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m scripts.compare --known data/generated/A --lake data/lake
   ```

   The script prints ten lines. Each line starts with `PASS` or `FAIL`:

   ```text
   PASS  events_in                  expected 50000  observed 50000
   PASS  silver                     expected 48630  observed 48630
   PASS  quarantine                 expected 920  observed 920
   PASS  quarantine_by_reason       expected {...}  observed {...}
   PASS  duplicates                 expected 450  observed 450
   PASS  reconciliation             expected 50000  observed 50000
   PASS  late_flagged               expected 843  observed 843
   PASS  trades                     expected 34380  observed 34380
   PASS  active_trades              expected 32311  observed 32311
   PASS  current_state_mismatches   expected 0  observed 0
   ```

   On the `quarantine_by_reason` line, both braces hold the same four counts. The order of the keys can differ. The script exits with code 0 when all ten lines start with `PASS`. A `FAIL` line gives exit code 1.

You now have the result. The Delta tables are in `data/lake`. To look at one, see [How to look at a table](#how-to-look-at-a-table).

When Spark exits on Windows, it may print errors about a temporary folder that it cannot delete. In this project's runs, these messages came after the results were printed and did not change them.

To run the tutorial again from the start, delete the folders `data/generated` and `data/lake` first. Bronze does not read a file again after the file has changed, and the checkpoint in `data/lake` belongs to the old file.

## How-to guides

### How to run the tests

Prerequisites: steps 1 to 4 of the tutorial. The leak-scan test needs Git.

1. Run the tests.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m pytest tests -q
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m pytest tests -q
   ```

2. Wait. The full run takes about 9 minutes, because many tests start Spark.

Check that it worked: the last line reports only `passed`, for example `184 passed`. The number grows when tests are added. On Windows, `taskkill` messages can appear before the last line. You can ignore them. Three tests skip when PowerShell is not installed.

### How to look at a table

Prerequisites: the tutorial.

1. Print the first rows of the Gold report. Replace `gold_report` with `silver`, `quarantine`, `duplicates`, `current_state` or `bronze` to see another table.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -c "from src.common.spark import get_spark; s = get_spark(); s.sparkContext.setLogLevel('ERROR'); s.read.format('delta').load('data/lake/gold_report').orderBy('report_version', 'book_id', 'currency').show(5, False)"
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -c "from src.common.spark import get_spark; s = get_spark(); s.sparkContext.setLogLevel('ERROR'); s.read.format('delta').load('data/lake/gold_report').orderBy('report_version', 'book_id', 'currency').show(5, False)"
   ```

Check that it worked: a table appears with the columns `cutoff`, `report_version`, `published_at`, `book_id`, `currency`, `active_trades`, `total_quantity` and `notional`.

### How to add events to the lake

Bronze reads new files only, and it identifies a file by its path. A file that you change in place, or copy to a new path, is not handled as you may expect. Add events as new files in a folder that holds only event files.

Prerequisites: the tutorial.

1. Create a folder for the events, for example `data/landing`. Do not use `data/generated/A`: it also holds the known answer files, and the run would read them as events.
2. Copy `data/generated/A/events.ndjson` to `data/landing/part-1.ndjson`.
3. Run the pipeline on the folder, with a new output folder. The old `data/lake` already read the events from another path.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m src.pipeline.run_local --events data/landing --out data/lake2
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m src.pipeline.run_local --events data/landing --out data/lake2
   ```

4. Put a new file with more events in `data/landing`, for example `part-2.ndjson`. Run the same command again.

Check that it worked: `events_in` grows by the number of lines in the new file. Run the command a third time without a new file: no value changes.

### How to prove that a rebuild gives the same tables

Prerequisites: the tutorial.

The rebuild deletes every table in `data/lake`, including the Gold report, and the checkpoint. It then builds them again from the raw file. This also deletes the Gold version history that earlier runs added. Use the same `--events` value as in the run that made the lake. Events that arrived later in other files are not part of the rebuild.

1. Run the rebuild. It compares a fingerprint of each table before and after.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m src.pipeline.replay
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m src.pipeline.replay
   ```

Check that it worked: all six lines end with `SAME`.

```text
bronze         before 7913cad91d4d  after 7913cad91d4d  SAME
silver         before bdfefa98d802  after bdfefa98d802  SAME
quarantine     before 6525b5d673a5  after 6525b5d673a5  SAME
duplicates     before 5075cff5ad23  after 5075cff5ad23  SAME
current_state  before 34aadf0a841c  after 34aadf0a841c  SAME
gold_report    before a3a9f890e6f9  after a3a9f890e6f9  SAME
```

If a lake was made by an older version of this repository, the Bronze and quarantine lines can show `DIFFERENT`. Run the rebuild a second time to compare two runs of the same version.

### How to check the repository for leaks

Prerequisites: Git, and steps 1 to 4 of the tutorial.

1. Run the scan.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m scripts.leak_scan
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m scripts.leak_scan
   ```

Check that it worked: the scan prints nothing and exits with code 0. It looks for GUIDs, key strings, connection strings, workspace URLs, email addresses, Databricks tokens, workspace numbers and account IDs in tracked files. It finds known patterns only.

### How to clean run output before you save it

Prerequisites: steps 1 to 4 of the tutorial.

1. Save the output of a command to a file. For example, in a PowerShell terminal: `COMMAND > run.txt`. Windows PowerShell 5.1 writes UTF-16 with `>`. The tool reads UTF-16 and UTF-8.
2. Clean the file. Replace `run.txt` and `run.clean.txt` with your file names.

   **Windows (PowerShell):**

   ```powershell
   .venv\Scripts\python.exe -m scripts.redact run.txt run.clean.txt
   ```

   **macOS or Linux:**

   ```bash
   .venv/bin/python -m scripts.redact run.txt run.clean.txt
   ```

Check that it worked: `run.clean.txt` has labels such as `<guid>`, `<email>` and `<local>` where `run.txt` had IDs, emails, keys, workspace URLs, or your user name, computer name and home path. Read the clean file before you share it. The tool finds known patterns only.

### How to create and delete the Azure resources

The pipeline ran on Azure once (see [the run results](docs/azure-run-results.md)). The scripts create the resources it needs. The scripts do not create the Unity Catalog storage credential and external location. A person makes them in the workspace. They outlive the workspace: delete the external location and the storage credential before you delete the resource group.

Prerequisites: the Azure CLI, an Azure subscription, and a budget alert on that subscription. Create the budget alert in the Azure portal before you create anything. A budget alert does not stop spending, and it can lag by hours. The scripts never sign in for you.

1. Sign in. If your account is a guest in another tenant, add `--tenant TENANT_ID`. Replace `TENANT_ID` with your tenant ID. Never commit it.

   ```powershell
   az login
   ```

2. Select the subscription. The setup script stops unless the default subscription is named `Azure for Students`. To use another name, pass `-Subscription NAME` to the script.

   ```powershell
   az account set --subscription "Azure for Students"
   ```

3. Run the setup as a dry run. It prints the commands and creates nothing.

   ```powershell
   .\infra\setup.ps1 -NoEventHubs
   ```

4. Check the commands. Each one must name the resource group `rg-trade-pipeline`, your subscription's region and the tags `project`, `owner` and `delete-by`.
5. Create the resources. Add `-Go`.

   ```powershell
   .\infra\setup.ps1 -NoEventHubs -Go
   ```

   `-NoEventHubs` creates the resource group, a storage account with a container, and a Databricks workspace. Without it, the script also creates an Event Hubs namespace with Capture. Event Hubs bills by the hour. `-OnlyEventHubs` adds only the Event Hubs namespace and hub to a group that already exists. Capture needs the role `Storage Blob Data Owner` on the storage account, and the script reminds you. You cannot use `-NoEventHubs` and `-OnlyEventHubs` together.

6. When you finish, run the teardown as a dry run. It prints the delete command.

   ```powershell
   .\infra\teardown.ps1
   ```

7. Delete everything. Add `-Go`.

   ```powershell
   .\infra\teardown.ps1 -Go
   ```

8. Wait a few minutes. The command returns at once and the delete continues in the background.

Check that it worked: `az group list` no longer shows `rg-trade-pipeline`. The Databricks managed resource group, named `databricks-rg-...`, can stay for several more minutes. If it stays, delete it with `az group delete --name NAME --yes`. Replace `NAME` with its name.

To delete only Event Hubs at the end of a session, run `.\infra\teardown.ps1 -EventHubsOnly -Go`. A mistyped switch stops the script.

## Reference

### Commands

| Command | What it does | Needs internet |
|---|---|---|
| `python -m src.generator.generate --profile A --n 50000 --seed 42` | Writes events and the known answer to `data/generated/A`. Options: `--profile {A,B}`, `--n`, `--seed`, `--out`. | No |
| `python -m src.generator.send --events FILE --topic trade-events --start 0 --count 100` | Sends events to Event Hubs, all to partition 0, in file order. Needs `confluent-kafka` (not in `requirements.txt`), and the variables `EVENTHUBS_NAMESPACE` and `EVENTHUBS_CONNECTION_STRING`. Tested with a fake producer. In the Azure probe it delivered 100 events. | Yes |
| `python -m src.pipeline.run_local` | Builds all tables in `data/lake`. Options: `--events` (a file or a folder of NDJSON files), `--out`. In Python, `run()` also takes a streaming DataFrame and a storage address (`abfss://...`). The storage address is tested with a `file://` address only. | First run only |
| `python -m src.pipeline.replay` | Deletes the tables, rebuilds them, and compares fingerprints. Options: `--events`, `--out`. | First Spark run only |
| `python -m scripts.compare` | Compares the tables with the known answer. Options: `--known`, `--lake`. | First Spark run only |
| `python -m scripts.leak_scan` | Scans tracked files for secrets and identifiers. Needs Git. | No |
| `python -m scripts.redact SOURCE TARGET` | Replaces known identifiers in a text file with labels. | No |
| `python -m pytest tests -q` | Runs the tests. | First Spark run only |
| `ruff check .` | Lints the code. | No |
| `.\infra\setup.ps1`, `.\infra\teardown.ps1` | Create and delete Azure resources. Dry run unless you pass `-Go`. | Yes |

### Files and folders

| Path | Content |
|---|---|
| `src/generator/` | The event generator and the known answer. |
| `src/pipeline/` | Bronze, Silver, current state, Gold, the local run, the rebuild, and the Kafka and Capture readers. |
| `src/common/` | The Spark session, the reference data loader, and Windows fixes for local Spark. |
| `scripts/` | The comparison, the leak scan, and the redaction tool. |
| `config/faults.toml` | Fault rates and the data profiles A and B. |
| `config/pipeline.toml` | Settings for the Gold report. |
| `data/reference/` | Reference lists: books, counterparties, currencies, instruments. All fake. |
| `infra/` | Azure setup and teardown scripts. |
| `notebooks/` | The Databricks notebooks: the Azure probe and the full run (both ran). |
| `tests/` | The tests. They compare output with the generator's known answer. |
| `docs/` | The architecture page and its diagram, and the Azure probe results. |

Generated data goes to `data/generated/` and `data/lake/`. Git ignores both.

### Configuration

`config/faults.toml` sets the data that the generator makes. Section `[run]` holds `start_utc`, `duration_minutes` and `late_after_seconds`. Section `[mix]` holds the shares of amended and cancelled trades. Sections `[profile_a]` and `[profile_b]` hold the fault rates: duplicates, out-of-order events, late events, bad records, conflicts and one outage. The same seed always gives the same events.

The generator uses `late_after_seconds` to count late events in the known answer. The pipeline does not read it: `build_silver` in `src/pipeline/silver.py` uses 300 seconds. If you change one value, change the other, or the comparison fails.

`config/pipeline.toml` sets the Gold report: `start_utc`, `cutoff_minutes` (15) and `first_publish_minutes` (5). The value of `start_utc` must equal the one in `faults.toml`. A test checks this.

### Event contract

Each event has these fields. The generator also writes `arrival_time_utc` into each line. The pipeline reads it as `ingest_time`.

| Field | Type | Rule |
|---|---|---|
| `event_id` | text | A later event with the same value is a duplicate if its content is the same, and a conflict if its content differs. The first event wins. |
| `trade_id` | text | |
| `trade_version` | integer | 1 or more. The highest version wins. |
| `action` | text | `NEW`, `AMEND` or `CANCEL`. |
| `instrument_id` | text | Must exist in `data/reference/instruments.csv`. |
| `counterparty_id` | text | Must exist in `data/reference/counterparties.csv`. |
| `book_id` | text | Must exist in `data/reference/books.csv`. |
| `side` | text | `BUY` or `SELL`. |
| `quantity` | integer | More than 0. |
| `price` | decimal | More than 0. |
| `currency` | text | Must exist in `data/reference/currencies.csv`. |
| `event_time_utc` | timestamp | Format `2026-11-02T14:00:00Z`. |

The pipeline adds `ingest_time` and `source` (`synthetic`).

### Tables

The [architecture page](docs/architecture.md#layers) lists the columns of each table. The Gold report never adds amounts in different currencies.

### Error messages

| Message starts with | Meaning | What to do |
|---|---|---|
| `... exists but has no checkpoint.` | `run_local` found a Bronze table without its checkpoint folder. A new stream would read every event again and double Bronze. | Restore the folder `data/lake/_checkpoint/bronze`, or delete the whole `data/lake` folder and run again. Deleting it also deletes the Gold version history. |
| `FAIL  <check> expected ... observed ...` | A table differs from the known answer. | Read the check name. Find the cause, then run `python -m scripts.compare` again. The exit code is 1. |
| `<rule>: <path>:<line>` from the leak scan | A tracked file matches a leak rule. | Delete the value from the file. The exit code is 1. |
| `HADOOP_HOME and hadoop.home.dir are unset` | Windows only. Spark cannot find `winutils.exe` and `hadoop.dll`. | See [Set up Hadoop files on Windows](#set-up-hadoop-files-on-windows). |
| `UnsatisfiedLinkError` | Windows only. Java cannot load `hadoop.dll` from `PATH`. | Start Spark through `get_spark()` in `src/common/spark.py`. It adds `%HADOOP_HOME%\bin` to `PATH`. |
| `Python worker exited unexpectedly` | Windows only. A Python worker crashed. | Start Spark through `get_spark()`. It sets up a launcher in `src/common/winspark`. See FAILURES.md, "Python worker crashes on Windows". |
| `Not signed in. Run az login first.` | The Azure CLI has no session. | Run `az login`. |
| `The default subscription is '...'. Expected '...'.` | The Azure CLI default is another subscription. | Run the `az account set` command in the message, or pass `-Subscription`. |
| `Command failed (exit N)` | An `az` command in `setup.ps1` or `teardown.ps1` failed. | Read the `az` error above it. Check the quota and the region. |
| `A parameter cannot be found` | You mistyped a switch. | Check the spelling. The script did nothing. |
| `No Event Hubs namespace found. Nothing to delete.` | `teardown.ps1 -EventHubsOnly` found nothing to delete. | None. |
| A raw Java or Python traceback | Anything else. | Read the last lines first. Search [FAILURES.md](FAILURES.md) for the exception name. |

### Limits and costs

- The 50,000-event run writes about 30 MB of files to `data/generated` and `data/lake`.
- The full test run takes about 9 minutes on the machine that built the project.
- The Azure resources cost money while they exist. Delete them after each session.

## Why it works this way

- **Fake data and a known answer.** Real data has no answer key. With a generator, the right result is known before the pipeline runs. The trade-off: the faults are assumptions, so the results prove the logic, not the real world.
- **A check that does not use the pipeline's counts.** `scripts/compare.py` reads the known answer files and the tables. A test drops one event before Bronze: the pipeline's own check says 0 events lost, and the script fails. The trade-off: the known answer comes from the same generator that made the events. The check is independent of the pipeline, not of the generator.
- **Version order, not arrival order.** In real feeds, a late message can hold an older version. The newest version must win. The trade-off: every trade needs a version number.
- **Late events are kept.** The pipeline flags them and never drops them. A late event that changes a published report makes a new numbered Gold version. The trade-off: reports can change after publishing, so each version is kept.
- **Bronze incremental, the rest recomputed.** Bronze reads only new events. Silver, quarantine, duplicates, current state and the Gold inputs are recomputed from all of Bronze. Recomputing is simple and gives the same answer every time. The trade-off: it does more work than an incremental update.
- **Micro-batches, not a continuous stream.** The student subscription blocks classic clusters, so the Azure plan uses serverless compute. The pipeline runs micro-batches that stop when all new events are read. A Kafka stream with a checkpoint on storage ran on serverless with 50,000 events, and a run after a pause resumed at the right offset. The project makes no claim of real-time processing.
- **A hash as the Bronze id.** The id is a hash of the raw text, so a rebuild from an archive gives the same ids. The trade-off: two identical lines share an id.

[DECISIONS.md](DECISIONS.md) has the full list. [FAILURES.md](FAILURES.md) lists each real failure and its fix.

## Claims and evidence

The generator knows the right answer. The tests compare the pipeline output with it. A **PASS** claim has a test or a run that shows it. An **UNVERIFIED** claim has code or a plan, and no run shows it.

| Claim | Expected | Observed | Status | Evidence |
|---|---|---|---|---|
| The counts add up: every event is counted once | 50,000 in = Silver + quarantine + duplicates | 48,630 + 920 + 450 = 50,000 on the 50,000-event run | PASS (local run) | The compare script. `tests/test_run_local.py` on 5,000 events |
| The check does not use the pipeline's own counts | The compare script reads the known answer files, not the pipeline's counts | 10 of 10 checks pass on the 50,000-event run. In a test with 3,000 events, one event is dropped before Bronze: the pipeline's own check says 0 lost, the script fails. | PASS (local run) | `scripts/compare.py`, `tests/test_compare.py` |
| A rebuild gives equal tables | 6 of 6 tables equal | 6 of 6 equal, in a test (5,000 events) and on the 50,000-event run | PASS (local run) | `tests/test_replay.py`, the replay command |
| A late event makes a new Gold version | Version 2 after late events | 2 Gold versions | PASS (local run) | `tests/test_gold.py` |
| Gold versions survive between runs | A later run keeps every published version. A changed number becomes a new version. | A second run with a changed input keeps version 1 and adds version 2. The same input again adds nothing. The rebuild is the exception: it deletes Gold. | PASS (local run) | `tests/test_gold_append_only.py` |
| Bronze reads new files only and resumes from a checkpoint | The second run adds no old events. Silver from incremental Bronze equals Silver from one read. | Equal in 5 tests on the Bronze reader and 3 tests on `run_local`, with a folder source. A Bronze table without a checkpoint is refused. | PASS (local, folder source) | `tests/test_bronze_stream.py`, `tests/test_run_local_stream.py` |
| A failed micro-batch is redone from the checkpoint | After a failed batch and a rerun, Bronze has every event once | In one test: 6 rows after the failed run, 9 after the rerun, 9 different ids, equal to a clean read. The failure comes before the commit. | PASS (local, folder source) | `tests/test_bronze_crash.py` |
| A batch read from Event Hubs through Kafka works on serverless | Every sent event is read | 100 sent from a laptop, 100 read on serverless (SASL/PLAIN, listen-only policy) | PASS (Azure probe, 100 events) | [Probe results](docs/azure-probe-results.md), P3 |
| Bronze reads a Kafka stream with a checkpoint | Every sent event arrives once, with the `ingest_time` rule applied. | 50,000 events from a real Event Hubs, read in six micro-batches with a checkpoint on storage. Offsets 0 to 49,999, none twice, none missing. | PASS (Azure run, 50,000 events, one partition) | [Run results](docs/azure-run-results.md), R3. Stand-in tests: `tests/test_kafka_read.py`, `tests/test_kafka_stream.py`, `tests/test_send.py` |
| A run after a pause reads only the new events | The second run starts where the first ended | The first run ended at offset 25,000. The next micro-batch ended at 35,000. Six micro-batches in all. | PASS (Azure run) | [Run results](docs/azure-run-results.md), R2, R3 |
| `maxOffsetsPerTrigger` limits a micro-batch on a real source | No batch is larger than the limit | Limit 10,000. The end offsets are 10,000, 20,000, 25,000, 35,000, 45,000 and 50,000. | PASS (Azure run) | [Run results](docs/azure-run-results.md) |
| A Bronze checkpoint on storage resumes on serverless | The second run reads nothing new | 2,000 events in, second run equal, with a folder source and a Delta checkpoint on ADLS | PASS (Azure probe) | [Probe results](docs/azure-probe-results.md), P2 |
| The full pipeline runs on Azure serverless | Same counts as the local run, and equal to the known answer | 10 of 10 lines PASS (50,000 in, 48,630 Silver, 920 quarantine, 450 duplicates, 843 late, 34,380 trades, 32,311 active, 0 lost). The Gold table has 3 versions, the local run 2. The latest version is equal. The reason for the third version was not examined. | PASS (Azure run, 50,000 events) | [Run results](docs/azure-run-results.md), R2, R4 |
| A rebuild from a Capture-format archive gives equal Bronze and Silver | Equal fingerprints | Equal in 3 tests on a fake archive. The archive follows the documented Capture layout, written from general knowledge, not from a real file. | PASS (local fixture) | `tests/test_capture_source.py` |
| A real Capture file reads and matches the Kafka rows | Same events, and the offset field is known | 100 of 100 matched. `SequenceNumber` equals the Kafka offset. `Offset` does not. | PASS (Azure probe) | [Probe results](docs/azure-probe-results.md), P4 |
| A lake built from a Capture-format archive, read as a stream, equals a lake built from the file | Equal fingerprints for Bronze (shared columns), Silver, quarantine, duplicates, current state and the latest Gold version | 6 of 6 equal on 2,000 events. A second run adds nothing. A new file adds only its events. | PASS (local fixture) | `tests/test_lake_compare.py`, `tests/test_capture_source.py` |
| A rebuild from a real Event Hubs Capture archive gives equal tables | Equal fingerprints | 50,000 events in the archive. 6 of 6 tables the same as the Kafka lake. The rebuild reads the archive in one batch, because serverless does not allow `avro` as a streaming source. | PASS (Azure run, 50,000 events) | [Run results](docs/azure-run-results.md), R5 |

### Not proven

- More than one run on Azure. The 50,000-event run happened once, with one partition. A repeat run, another seed and more partitions did not run.
- The cost. The Azure billing data of this subscription type shows no cost values. The hub ran 4 hours 52 minutes.
- Access control on the external location. No denied-access test was run.
- A rebuild from the archive as a stream. Serverless does not allow it.
- The end-to-end run and the comparison script on profile B or on other seeds. Profile B is tested for the generator, Silver, current state and Gold only.
- Behavior with more than one Kafka partition.
- Recovery after a crash inside a Delta commit, after a killed process, or on serverless. Only a failed micro-batch before the commit is tested.
- Any result on real data.
- macOS. Ubuntu CI passes. The 50,000-event run and the comparison ran on Windows only.

## What is next

1. Find out why the Gold table has 3 versions on Azure and 2 locally.
2. Run the end-to-end run and the comparison on profile B and on other seeds.
3. Run the tests on macOS. Ubuntu already passes in CI.

## License

Apache License 2.0. See [LICENSE](LICENSE).
