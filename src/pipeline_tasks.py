"""File-bound task functions shared by Airflow; XCom carries metadata only."""
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def ingest_and_validate() -> dict:
    from .streaming import produce_jsonl, consume_validated
    sent = produce_jsonl(str(ROOT / "data/support_events.jsonl"))
    accepted, rejected = consume_validated(sent)
    (ARTIFACTS / "staging").mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "quarantine").mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "staging/accepted.json").write_text(json.dumps(accepted), encoding="utf-8")
    (ARTIFACTS / "quarantine/rejected.json").write_text(
        json.dumps(rejected, indent=2, default=str), encoding="utf-8")
    return {"sent": sent, "accepted": len(accepted), "rejected": len(rejected)}


def validate_quality(metadata: dict) -> dict:
    from .quality import run_quality_gate
    rows = json.loads((ARTIFACTS / "staging/accepted.json").read_text(encoding="utf-8"))
    passed, _ = run_quality_gate(pd.DataFrame(rows))
    if not passed:
        raise ValueError("Great Expectations quality gate failed")
    return {**metadata, "quality_passed": True}


def build_lakehouse(metadata: dict) -> dict:
    from .lakehouse import create_spark, write_bronze, upsert_silver, build_gold
    rows = json.loads((ARTIFACTS / "staging/accepted.json").read_text(encoding="utf-8"))
    spark = create_spark()
    base = ARTIFACTS / "lakehouse"
    bronze = write_bronze(spark, rows, str(base / "bronze/support_events"))
    silver = upsert_silver(spark, bronze, str(base / "silver/support_events"))
    gold = build_gold(silver, str(base / "gold/support_metrics"))
    counts = {"bronze_rows": bronze.count(), "silver_rows": silver.count(), "gold_rows": gold.count()}
    spark.stop()
    return {**metadata, **counts}


def refresh_rag_index(metadata: dict) -> dict:
    from .rag import chunk_articles, HybridRAG
    articles = json.loads((ROOT / "data/knowledge_base.json").read_text(encoding="utf-8"))
    chunks = chunk_articles(articles)
    index = HybridRAG(chunks, persist_path=str(ARTIFACTS / "chroma"))
    return {**metadata, "indexed_chunks": index.collection.count()}


def emit_complete(metadata: dict) -> dict:
    from openlineage.client.event_v2 import RunState
    from .lineage import PipelineLineage
    emitter = PipelineLineage(str(ARTIFACTS / "lineage/openlineage.log"))
    emitter.emit(RunState.START)
    emitter.emit(RunState.COMPLETE)
    return {**metadata, "lineage_state": "COMPLETE"}
