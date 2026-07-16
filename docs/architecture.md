# Architecture notes

## Reliability model

The Kafka consumer uses manual offset commits. Processing is at-least-once; duplicate delivery is neutralized by the Silver Delta `MERGE` key (`event_id`). A malformed event never enters Bronze and is instead stored with Kafka coordinates and validation errors for replay.

## Medallion semantics

- **Bronze:** accepted events in their source shape plus ingestion timestamp.
- **Silver:** deduplicated, PII-masked support events with processing timestamp.
- **Gold:** aggregates by priority and channel for operational reporting.

## Governance boundary

The contract rejects unknown fields and invalid enum values. PII is masked before Silver and before any vector indexing. Synthetic input is used so the repository itself contains no customer PII.

## RAG evaluation

The demonstration checks retrieval traceability through article IDs. A production evaluation set should additionally measure context precision, context recall, answer faithfulness, and answer relevance using a fixed golden dataset.

