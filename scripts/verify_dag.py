"""Offline structural check for the activity DAGs.

This does NOT execute tasks. It only imports the DAG file with the local Task SDK
and reports the structure Airflow's DAG processor should discover. Use it to catch
typos and dependency mistakes before opening the Web UI.

Usage:
    python scripts/verify_dag.py dags/reliable_pipeline_demo.py
    make verify-dag

Exit code is 0 when the DAG parses, 1 otherwise.
"""

from __future__ import annotations

import importlib.util
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

EXPECTED = {
    "reliable_pipeline_demo.py": {
        "dag_id": "reliable_pipeline_demo",
        "tasks": {"start", "extract_data", "validate_data", "transform_data", "load_data"},
    },
}


def load_dag(path: Path):
    """Import the module and capture every DAG the factory instantiates.

    The activity DAG calls ``reliable_pipeline_demo()`` at module level without
    assigning the result, so the object cannot be found by scanning module
    attributes. Instead we spy on ``DAG.__init__`` while the module executes.
    """
    from airflow.sdk.definitions import dag as dag_module

    created: list = []
    original_init = dag_module.DAG.__init__

    def spy(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    spec = importlib.util.spec_from_file_location("dag_under_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)

    dag_module.DAG.__init__ = spy
    try:
        spec.loader.exec_module(module)
    finally:
        dag_module.DAG.__init__ = original_init

    if not created:
        raise RuntimeError(f"No DAG object was instantiated in {path}")
    return created[-1]


def main(argv: list[str]) -> int:
    target = Path(argv[1]) if len(argv) > 1 else Path("dags/reliable_pipeline_demo.py")
    if not target.exists():
        print(f"ERROR: file not found: {target}")
        return 1

    print(f"Verifying {target} ...")
    try:
        from airflow.sdk import dag, task  # noqa: F401
        from airflow.sdk.definitions.dag import DAG  # noqa: F401

        print("  airflow.sdk import: OK")
    except Exception as exc:  # noqa: BLE001
        print(f"  airflow.sdk import: FAILED -> {exc}")
        print("  Did you install the Task SDK with the Airflow 3.1.8 constraints file?")
        print("  See README.md section 4.")
        return 1

    try:
        parsed = load_dag(target)
    except Exception as exc:  # noqa: BLE001
        print(f"  DAG parsing: FAILED -> {type(exc).__name__}: {exc}")
        return 1

    print(f"  DAG parsing: OK -> dag_id={parsed.dag_id}")
    print(f"  catchup={parsed.catchup}  tags={sorted(parsed.tags)}")

    task_ids = set(parsed.task_ids)
    edges = sorted(
        (up, task.task_id) for up in parsed.task_dict for task in parsed.task_dict[up].downstream_list
    )

    print(f"  tasks ({len(task_ids)}):")
    for tid in sorted(task_ids):
        print(f"    - {tid}")
    print(f"  dependency edges ({len(edges)}):")
    for up, down in edges:
        print(f"    {up} >> {down}")

    exp = EXPECTED.get(target.name)
    if exp is None:
        print("\nNo expectation registered for this file. Structural check only.")
        return 0
    problems = []
    if parsed.dag_id != exp["dag_id"]:
        problems.append(f"dag_id is {parsed.dag_id!r}, expected {exp['dag_id']!r}")
    if task_ids != exp["tasks"]:
        missing = exp["tasks"] - task_ids
        extra = task_ids - exp["tasks"]
        if missing:
            problems.append(f"missing tasks: {sorted(missing)}")
        if extra:
            problems.append(f"unexpected tasks: {sorted(extra)}")
    if len(edges) != len(exp["tasks"]) - 1:
        problems.append(f"expected {len(exp['tasks']) - 1} edges, found {len(edges)}")

    if problems:
        print("\nFAILED:")
        for p in problems:
            print(f"  - {p}")
        return 1

    print(f"\nPASSED: {len(task_ids)} tasks and {len(edges)} edges match the design.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))