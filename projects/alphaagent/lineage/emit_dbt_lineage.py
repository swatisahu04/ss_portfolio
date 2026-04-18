"""
Post-dbt-run lineage emitter.

Reads `dbt_project/target/manifest.json` and emits OpenLineage events
so Marquez shows the full DAG (sources → staging → intermediate → marts).

Airflow can call this as a post-task; the Makefile `transform` target
can call it via `make lineage` if desired.
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import httpx

MANIFEST = Path(__file__).resolve().parents[1] / "dbt_project" / "target" / "manifest.json"
LINEAGE_URL = "http://localhost:5000/api/v1/lineage"
NAMESPACE = "alphaagent"


def post_event(payload: dict) -> None:
    try:
        httpx.post(LINEAGE_URL, json=payload, timeout=3.0)
    except Exception as e:
        print(f"  ! emit failed: {e}")


def main() -> None:
    if not MANIFEST.exists():
        print(f"! {MANIFEST} missing — run `dbt run` first.")
        return

    manifest = json.loads(MANIFEST.read_text())
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    run_id = f"dbt-{int(time.time())}"
    print(f"→ emitting lineage for {len(nodes)} models")

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        out_name = f"{node['schema']}.{node['name']}"
        inputs = []
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep in nodes:
                d = nodes[dep]
                inputs.append(f"{d['schema']}.{d['name']}")
            elif dep in sources:
                src = sources[dep]
                inputs.append(f"{src['schema']}.{src['name']}")

        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.utcnow().isoformat() + "Z",
            "run": {"runId": run_id},
            "job": {"namespace": NAMESPACE, "name": f"dbt.{node['name']}"},
            "inputs": [{"namespace": f"postgres://{NAMESPACE}", "name": i} for i in inputs],
            "outputs": [{"namespace": f"postgres://{NAMESPACE}", "name": out_name}],
            "producer": "https://github.com/you/alphaagent/dbt",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        }
        post_event(event)

    print(f"✓ {len(nodes)} lineage events emitted")


if __name__ == "__main__":
    main()
