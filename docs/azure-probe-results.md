# Azure probe results

This page records the first run of the pipeline code on real Azure services. The probe is small: 100 events and four checks. It is not the full 50,000-event run. Dates are 2026-11-05. The data is fake.

The probe ran the notebook [`notebooks/azure_probe.py`](../notebooks/azure_probe.py) on Databricks serverless compute. The output below was copied from the notebook. It holds no secret and no account identifier.

## What was created

| Resource | Setting |
|---|---|
| Storage account | Data Lake Gen2 (hierarchical namespace on), container `archive` |
| Databricks workspace | Premium tier, serverless compute |
| Unity Catalog | One storage credential (managed identity through an access connector) and one external location for the `archive` container |
| Event Hubs namespace | Standard tier, 1 throughput unit, Kafka endpoint on |
| Event hub | 2 partitions, Capture every 60 seconds, empty files skipped |

The hub existed for 47 minutes. After the probe, `infra/teardown.ps1` and a direct delete of the Databricks managed group removed everything. `az resource list` then showed only the free network watcher.

## Results

| Check | Result | What it shows |
|---|---|---|
| P1 | PASS (1 entry) | Serverless lists the container through the external location. |
| P2 | PASS (2,000 events in, second run equal) | Bronze with a checkpoint on storage resumes on serverless. The second run reads nothing new. |
| P3 | PASS (100 events) | Serverless reads the events from Event Hubs through the Kafka endpoint, with SASL/PLAIN and the `kafkashaded.` login module. A listen-only policy on the hub is enough. |
| P4 | PASS (100 of 100 matched) | A real Capture file reads, and its rows match the Kafka rows. |

The sender (`src/generator/send.py`) delivered 100 events from a laptop with a send-only policy. Capture wrote one Avro file at `{namespace}/trade-events/0/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}.avro`. It was 38,940 bytes. The hour in the path is UTC.

## Capture file schema

The schema was read from the Avro header of the downloaded file. The codec was `null`.

| Field | Type |
|---|---|
| `SequenceNumber` | long |
| `Offset` | string |
| `EnqueuedTimeUtc` | string |
| `SystemProperties` | map of long, double, string, bytes |
| `Properties` | map of long, double, string, bytes, null |
| `Body` | null or bytes |

## Offsets

For all 100 events, the Capture `SequenceNumber` equals the Kafka offset. The Capture `Offset` does not. A rebuild from Capture must match rows by `SequenceNumber` when it uses a position. Bronze still keys rows by the hash of the text.

## Failures during the probe

Each failure and its fix is in [FAILURES.md](../FAILURES.md): a helper script that reported success after an error, a notebook that asked the generator for too few events, and a stored secret with a line break that broke the Kafka login.

## What the probe does not show

- The full 50,000-event run, and the comparison with the known answer, on Azure.
- A streaming read from Kafka with a checkpoint, and `maxOffsetsPerTrigger`.
- The resume proof with a pause between two sends.
- A rebuild of all tables from a real Capture archive. P4 reads the file and matches rows. It does not rebuild the tables.
- More than one partition. All events went to partition 0.
- Cost. No invoice was read. The hub price comes from third-party price lists.
- A denied-access test for the external location.
- File events for the external location. Their set-up failed on a missing permission, and the project does not use them.
