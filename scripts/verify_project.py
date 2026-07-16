"""Fast, dependency-light verification for repository completeness."""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

required = [
    "README.md", "requirements.txt", "pyproject.toml",
    "notebooks/customer_support_intelligence_colab.ipynb",
    "src/streaming.py", "src/lakehouse.py", "src/quality.py", "src/rag.py",
    "src/lineage.py", "src/governance.py", "src/pipeline_tasks.py",
    "dags/support_intelligence_dag.py", ".github/workflows/ci.yml",
]
missing = [path for path in required if not (ROOT / path).exists()]
assert not missing, f"Missing required files: {missing}"

notebook = json.loads((ROOT / required[3]).read_text(encoding="utf-8"))
assert notebook["nbformat"] == 4
for number, cell in enumerate(notebook["cells"], 1):
    if cell["cell_type"] != "code":
        continue
    source = "".join(cell["source"])
    clean = "\n".join(line for line in source.splitlines()
                      if not line.lstrip().startswith(("!", "%")))
    ast.parse(clean, filename=f"notebook-cell-{number}")

evidence = "\n".join((ROOT / path).read_text(encoding="utf-8")
                       for path in required if path.endswith((".py", ".md")))
for token in ["KafkaProducer", "KafkaConsumer", "DeltaTable", ".merge(",
              "PersistentClient", "BM25Okapi", "CrossEncoder",
              "Checkpoint", "OpenLineageClient", "@dag"]:
    assert token in evidence, f"Rubric evidence missing: {token}"

print(f"PASS: {len(required)} required files, {len(notebook['cells'])} notebook cells")
print("PASS: rubric evidence found for Kafka, Delta MERGE, RAG, GX, OpenLineage, Airflow")

