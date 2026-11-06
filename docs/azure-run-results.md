# Azure full-run results

This page records the full run: 50,000 fake events through Event Hubs, Bronze, Silver, current state and Gold on Databricks serverless compute. Date: 2026-11-05. The first small run is on [the probe page](azure-probe-results.md).

The run used [`notebooks/azure_run.py`](../notebooks/azure_run.py). The generator wrote the known answer before the run. The output below was copied from the notebook. It holds no secret and no account identifier.

## What ran

| Step | What happened |
|---|---|
| Send | A laptop sent events 0 to 24,999 to partition 0 of the hub, then a pause, then events 25,000 to 49,999. |
| Read | Bronze read the hub through the Kafka endpoint, with a checkpoint on storage and a limit of 10,000 events per micro-batch. |
| Rebuild | A second lake was built from the Event Hubs Capture archive and compared with the first. |

## Results

| Check | Result | What it shows |
|---|---|---|
| R1 | PASS | Bronze read the first half from Kafka and wrote a checkpoint on storage. |
| R2 | PASS (50,000 in, 48,630 Silver, 920 quarantine, 450 duplicates, 843 late, 34,380 trades, 32,311 active, 0 lost) | The second run read the second half. The counts equal the local run. |
| R3 | PASS (50,000 rows, offsets 0 to 49,999, none twice, none missing) | Every Kafka offset is in Bronze once. |
| R4 | PASS (10 of 10 lines) | The tables equal the known answer that the generator wrote, including the quarantine reasons and 0 current-state mismatches. |
| R5 | PASS (50,000 events in the archive, 6 of 6 tables the same) | A lake rebuilt from the Capture archive equals the Kafka lake. Bronze is compared on the columns both sources have. Gold is compared on its latest version. |

The checkpoint on storage lists six micro-batches. Their end offsets on partition 0 are 10,000, 20,000, 25,000, 35,000, 45,000 and 50,000. This shows two things. The limit of 10,000 events per batch worked. The run after the pause started at 25,000, where the first run ended.

## What went wrong

The first attempts failed in four places. [FAILURES.md](../FAILURES.md) has the details.

- The code folder `scripts` was not uploaded, so an import failed.
- `awaitTermination()` never returned on serverless, although the stream had finished. The run waited 23 minutes twice. A loop that checks `isActive` replaced it.
- Serverless does not allow the `avro` format as a streaming source. The rebuild reads the archive in one batch instead.
- A notebook keeps old modules in memory after the files change. A Python restart is needed.

## Not shown

- The Gold table has 3 versions. The local run has 2. The Gold latest version equals the known answer and the rebuild. The reason for the third version was not examined.
- A rebuild from the archive was run as one batch. A rebuild as a stream is not possible on serverless.
- One partition holds all events. More than one partition is not tested.
- The cost was not read from an invoice. The billing data of this subscription type shows no cost values.
- Access control. A denied-access test was not run.

## Resources and teardown

The hub existed from 17:05 to 21:57 (4 hours 52 minutes). The planned limit was 3 hours. The run went over it, because the first R2 attempts hung. The Unity Catalog objects were deleted first, then the Event Hubs namespace, then the resource group.
