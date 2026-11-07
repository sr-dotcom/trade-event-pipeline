# Claims and evidence


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
| A batch read from Event Hubs through Kafka works on serverless | Every sent event is read | 100 sent from a laptop, 100 read on serverless (SASL/PLAIN, listen-only policy) | PASS (Azure probe, 100 events) | [Probe results](azure-probe-results.md), P3 |
| Bronze reads a Kafka stream with a checkpoint | Every sent event arrives once, with the `ingest_time` rule applied. | 50,000 events from a real Event Hubs, read in six micro-batches with a checkpoint on storage. Offsets 0 to 49,999, none twice, none missing. | PASS (Azure run, 50,000 events, one partition) | [Run results](azure-run-results.md), R3. Stand-in tests: `tests/test_kafka_read.py`, `tests/test_kafka_stream.py`, `tests/test_send.py` |
| A run after a pause reads only the new events | The second run starts where the first ended | The first run ended at offset 25,000. The next micro-batch ended at 35,000. Six micro-batches in all. | PASS (Azure run) | [Run results](azure-run-results.md), R2, R3 |
| `maxOffsetsPerTrigger` limits a micro-batch on a real source | No batch is larger than the limit | Limit 10,000. The end offsets are 10,000, 20,000, 25,000, 35,000, 45,000 and 50,000. | PASS (Azure run) | [Run results](azure-run-results.md) |
| A Bronze checkpoint on storage resumes on serverless | The second run reads nothing new | 2,000 events in, second run equal, with a folder source and a Delta checkpoint on ADLS | PASS (Azure probe) | [Probe results](azure-probe-results.md), P2 |
| The full pipeline runs on Azure serverless | Same counts as the local run, and equal to the known answer | 10 of 10 lines PASS (50,000 in, 48,630 Silver, 920 quarantine, 450 duplicates, 843 late, 34,380 trades, 32,311 active, 0 lost). The Gold table has 3 versions, because the 50,000 events came in two halves. A split run on a laptop gives the same 3 versions, and its latest version equals the single run. See [the run results](azure-run-results.md). | PASS (Azure run, 50,000 events) | [Run results](azure-run-results.md), R2, R4 |
| A rebuild from a Capture-format archive gives equal Bronze and Silver | Equal fingerprints | Equal in 3 tests on a fake archive. The archive follows the documented Capture layout, written from general knowledge, not from a real file. | PASS (local fixture) | `tests/test_capture_source.py` |
| A real Capture file reads and matches the Kafka rows | Same events, and the offset field is known | 100 of 100 matched. `SequenceNumber` equals the Kafka offset. `Offset` does not. | PASS (Azure probe) | [Probe results](azure-probe-results.md), P4 |
| A lake built from a Capture-format archive, read as a stream, equals a lake built from the file | Equal fingerprints for Bronze (shared columns), Silver, quarantine, duplicates, current state and the latest Gold version | 6 of 6 equal on 2,000 events. A second run adds nothing. A new file adds only its events. | PASS (local fixture) | `tests/test_lake_compare.py`, `tests/test_capture_source.py` |
| A rebuild from a real Event Hubs Capture archive gives equal tables | Equal fingerprints | 50,000 events in the archive. 6 of 6 tables the same as the Kafka lake. The rebuild reads the archive in one batch, because serverless does not allow `avro` as a streaming source. | PASS (Azure run, 50,000 events) | [Run results](azure-run-results.md), R5 |

### Not proven

- More than one run on Azure. The 50,000-event run happened once, with one partition. A repeat run, another seed and more partitions did not run.
- Where the Databricks cost came from. The probe and the full run cost about 7.2 USD in all, of which Databricks was about 6.0 USD. The share of the two hung attempts of R2 was not separated. See [What it cost](azure-run-results.md#what-it-cost).
- Access control on the external location. No denied-access test was run.
- A rebuild from the archive as a stream. Serverless does not allow it.
- The end-to-end run and the comparison script on profile B or on other seeds. Profile B is tested for the generator, Silver, current state and Gold only.
- Behavior with more than one Kafka partition.
- Recovery after a crash inside a Delta commit, after a killed process, or on serverless. Only a failed micro-batch before the commit is tested.
- Any result on real data.
- macOS. Ubuntu CI passes. The 50,000-event run and the comparison ran on Windows only.

## What is next

1. Run the end-to-end run and the comparison on profile B and on other seeds.
2. Run the tests on macOS. Ubuntu already passes in CI.
