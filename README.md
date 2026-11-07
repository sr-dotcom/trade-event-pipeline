# trade-event-pipeline

**A trade-event pipeline on Azure. It handles duplicates, late events, corrections and bad records, and it checks its own output against an answer that was written before the run.**

Why it matters: in trading, a record that is counted twice, or that arrives late, gives wrong numbers. This project shows how to catch those cases, keep a trail, and prove the result.

All data is fake. This is a learning project, built in about a week by one person. It is not production code.

Built with an AI coding assistant (Claude Code). The author chose the scope, ran every step on Azure, and reviewed every result.

[![CI](https://github.com/sr-dotcom/trade-event-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/sr-dotcom/trade-event-pipeline/actions/workflows/ci.yml)
[Interactive walkthrough (2 minutes)](https://sr-dotcom.github.io/trade-event-pipeline/) · [One-page summary (PDF)](docs/summary.pdf) · [Run results](docs/azure-run-results.md) · [Architecture](docs/architecture.md) · [Claims and evidence](docs/claims.md)

![Data flow: Generator to Event Hubs to Bronze, then Silver, Quarantine and Duplicates, then Current state, Gold and the Compare script. The Capture archive feeds a batch rebuild of Bronze. Every box is marked PASS. Three cards list what it proved, what it cost and what is not proven.](docs/images/architecture.png)

## Result

The full run was on 5 November 2026, on Azure Event Hubs and Azure Databricks.

| What was tested | Result |
|---|---|
| 50,000 fake events through Event Hubs to Databricks | All processed. 0 lost. |
| The output against the known answer | 10 of 10 checks equal |
| A rebuild from the saved archive | 6 of 6 tables equal. Damaged tables can be rebuilt with the same result. |
| A restart after a pause | It resumed at the right place. No event twice, none missing. |
| Tests | 185 tests, which also pass on Ubuntu in GitHub Actions (the badge above) |
| Cost | About 7 USD for the two Azure runs, including two hung attempts that were not separated. About 10 USD for the whole project. Everything was deleted after each run. |

## What is not proven

- **The test is self-made.** One generator makes the events, the faults and the answer key. The result shows that the pipeline does what the design says. It does not show that the design fits real feeds.
- **The data is fake.** There is no real market data and no client data.
- **It is a small test.** One lane of the message stream (one partition), 50,000 events, one run. More lanes, more events and other settings are untested.
- **No security test.** Nobody tested that a user without permission is blocked.
- **Not real time.** It reads in small batches. It is not production code, and it makes no claim about regulation or compliance.

[Claims and evidence](docs/claims.md) has the full list, and a test or a run behind every PASS.

## The problem it solves

Trade feeds are messy. The pipeline is built for four cases:

- **Duplicates.** The same event arrives twice. One is counted and the repeat is listed (450 in the run).
- **Late and out-of-order events.** The newest version of a trade wins, not the newest arrival. Late events are flagged and never dropped (843).
- **Bad records.** They go to a separate table with a reason. Nothing is deleted (920).
- **Reports that change.** A report is kept in numbered versions. A late event adds a new version. An old version is never edited.

## In plain words

- **Event Hubs:** the Azure service that receives a stream of events. **Capture:** an automatic copy of every event saved to storage.
- **Bronze, Silver, Gold:** three table layers. Bronze is the raw events. Silver is the cleaned events. Gold is the reports.
- **Quarantine:** the table for bad records, with the reason. **Current state:** the newest version of each trade.
- **Known answer:** files the generator writes before the run. They say how many records each table must hold.

## How it works

A generator makes 50,000 fake trade events and writes the right answer. The events go to Event Hubs, and Capture saves a copy. Databricks reads them into Bronze, then sorts them into Silver, Quarantine and Duplicates. Current state keeps the newest version of each trade, and Gold builds the versioned reports. A separate script compares every table with the known answer. It does not use any count that the pipeline made about itself.

## Built with

Azure Event Hubs (Kafka endpoint and Capture), Azure Databricks serverless, Unity Catalog with a managed identity, Delta Lake, PySpark Structured Streaming with checkpoints, PowerShell setup and teardown scripts, pytest and GitHub Actions.

## Read more

- [Run results](docs/azure-run-results.md): what ran on Azure, what failed, what it cost.
- [Guide](docs/guide.md): how to run it on a laptop, the commands, the reference.
- [Architecture](docs/architecture.md): the design and the rules.
- [Decisions](DECISIONS.md) and [Failures](FAILURES.md): each choice, and each real failure with its fix.

## License

Apache License 2.0. See [LICENSE](LICENSE).
