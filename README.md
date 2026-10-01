# airflow-etl

**U2 Activity-2 (Part 1) — Orchestrating ETL Pipelines with Apache Airflow**
Course: ETL (G01) · Academic Program: Data Engineering and Artificial Intelligence

This repository contains the local Apache Airflow **3.1.8** development environment
and the orchestration skeleton DAG required by Part 1 of the activity.

> Scope: the official Docker Compose configuration is appropriate for learning and
> local exploration. It is **not** a production deployment and does not provide the
> security guarantees required for production.

---

## 1. Target environment: macOS (MacBook)

The course guide was written for **Windows 11 + WSL2 + PowerShell**. This
repository is adapted for a MacBook. The Docker/Compose/Airflow side is
identical; only the host-shell commands differ.

| Guide (Windows) | This repo (macOS) | Notes |
| --- | --- | --- |
| WSL2 + `wsl --status` | *(not needed)* | macOS has no WSL. Docker Desktop runs a lightweight Linux VM. |
| PowerShell | Terminal.app / iTerm2 (`zsh`) | Default macOS shell. |
| `Invoke-WebRequest ... -OutFile` | `curl -fsSL ... -o ...` | `docker-compose.yaml` is already committed here. |
| `.\.venv\Scripts\Activate.ps1` | `source .venv/bin/activate` | POSIX layout. |
| `py -3.12 -m venv .venv` | `python3.12 -m venv .venv` | See §4 if 3.12 is not installed. |
| `py -m pip install ...` | `python -m pip install ...` | Inside the activated venv. |
| `docker compose ...` | `docker compose ...` | Identical — Docker Desktop ships Compose v2. |

A `Makefile` is provided so most steps can be run as `make <target>` without
typing Compose commands by hand. See §11.

### Requirements

- macOS 13 (Ventura) or newer — Apple Silicon (arm64) and Intel are both supported.
- **Docker Desktop for Mac** with the Docker engine running.
- **Python 3.12** for the local DAG-authoring virtual environment.
- **VS Code** + the *Python* extension (optional but expected by the activity).
- ~4 GB of memory allocated to Docker; 8 GB preferred.

---

## 2. Project structure

```
airflow-etl/
├── dags/                          # Airflow workflow definitions (scanned/parsed)
│   └── reliable_pipeline_demo.py  # Activity Part 1 DAG
├── data/                          # Pipeline input/output data (later activities)
├── src/                           # Reusable ETL + validation logic (separated from orchestration)
├── logs/                          # Execution logs synchronized host <-> containers
├── config/                        # Local Airflow config overrides (empty by default)
├── plugins/                       # Optional Airflow extensions (none in this activity)
├── .vscode/                       # Editor settings (points to .venv)
├── scripts/                       # Offline DAG structural checker (no Docker needed)
│   └── verify_dag.py
├── docker-compose.yaml            # Official local multi-container config for Airflow 3.1.8
├── .env                           # Docker Compose environment variables
├── requirements-dev.txt           # Local DAG-authoring deps (Task SDK), used with constraints
├── Makefile                       # Convenience targets (macOS/Linux)
└── README.md
```

> Engineering principle: the DAG should describe **orchestration**. Reusable ETL and
> validation logic should be separated from the DAG when appropriate.

---

## 3. Verify Docker Desktop

1. Install Docker Desktop for Mac from <https://www.docker.com/products/docker-desktop/>.
2. Start Docker Desktop and wait until the engine reports it is running
   (the menu-bar whale turns solid, not animated).
3. In **Settings → General**, confirm **Use the new Virtualization framework** is enabled
   (default on current versions). No WSL-style backend toggle exists on macOS.
4. Verify from Terminal:

```bash
docker --version
docker compose version
```

The official Airflow quick-start requires **Docker Compose 2.14.0 or newer**.

### Setup checkpoint

- [ ] Docker Desktop installed and engine running.
- [ ] `docker --version` returns a version without error.
- [ ] `docker compose version` reports **2.14.0 or newer**.
- [ ] At least 4 GB of memory allocated to Docker (Settings → Resources → Memory).

---

## 4. Local Python 3.12 environment (DAG authoring only)

Airflow runs inside Docker, but VS Code analyzes Python files with the **local**
interpreter. A lightweight local environment is therefore used to recognize the
libraries needed for DAG authoring.

```bash
# Confirm the Python version used inside the containers (Part A, Step 9)
docker compose exec airflow-scheduler python --version     # expect Python 3.12.x

# Check which Python versions are installed on the Mac
python3 --version
ls -1 /Library/Frameworks/Python.framework/Versions/ 2>/dev/null || true
```

If Python 3.12 is not installed:

```bash
brew install python@3.12
# or, without Homebrew, from https://www.python.org/downloads/macos/
```

Create and activate the virtual environment from the project root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python --version                 # expect Python 3.12.x
```

In VS Code:

1. Press <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>P</kbd> (macOS:
   <kbd>Cmd</kbd>+<kbd>Shift</kbd>+<kbd>P</kbd>).
2. Select **Python: Select Interpreter**.
3. Choose `.venv/bin/python`.

Install the **Task SDK** version associated with Airflow 3.1.8 — **use the constraints
file** (see the note below for why this matters):

```bash
python -m pip install "apache-airflow-task-sdk==1.1.8" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt"

python -c "import pendulum; from airflow.sdk import dag, task; print('Imports OK')"
```

Expected result:

```
Imports OK
```

Save that exact command as `requirements-dev.txt` in the project root so it is
reproducible:

```text
apache-airflow-task-sdk==1.1.8
```

…used together with `--constraint` as above.

### Verified notes on this step

These were verified while preparing this repository, and they differ slightly from the
course guide:

- **The constraints flag is not optional.** Without it, pip resolves
  `SQLAlchemy==2.1.1`, which is incompatible with `SQLAlchemy-Utils==0.42.1`. The
  symptom is a misleading error while importing the SDK:
  ```
  ImportError: cannot import name 'dag' from 'airflow.sdk'
  ```
  whose real cause is several frames deeper:
  ```
  AttributeError: module 'sqlalchemy.orm.attributes' has no attribute 'ScalarAttributeImpl'
  ```
  Airflow 3.1.8 pins `SQLAlchemy==2.0.48`, and the constraints file restores it.
- **The Task SDK is not as lightweight as the guide suggests.** `apache-airflow-task-sdk==1.1.8`
  declares a dependency on `apache-airflow-core>=3.1.7,<3.2.0`, so a large part of
  Airflow lands in `.venv` regardless. That is harmless for this activity, but do not
  be surprised by the install size or the `[alembic.runtime.plugins]` lines printed
  during import — they are not errors.
- **Do not** install the complete `apache-airflow` package deliberately on macOS. The
  venv exists only for DAG authoring and static analysis in VS Code.
- The Task SDK provides the `airflow.sdk` namespace and installs `pendulum` as a dependency.

> Key distinction: the local virtual environment supports code authoring and static
> analysis in VS Code. Apache Airflow and the DAG Runs execute inside Docker containers.

---

## 5. Start the Airflow environment

`docker-compose.yaml` and `.env` are already committed, so there is nothing to
download. From the project root:

```bash
# 1) First-time initialization: metadata DB, migrations, admin account
docker compose up airflow-init

# 2) Start all services in the background (-d keeps the terminal free)
docker compose up -d

# 3) Check status; allow time for the services to become healthy
docker compose ps
```

### What the services do

| Service | Responsibility |
| --- | --- |
| `airflow-scheduler` | Monitors workflows and creates runnable task instances when dependencies are satisfied. |
| `airflow-dag-processor` | Parses DAG files and makes their workflow definitions available to Airflow. |
| `airflow-apiserver` | Provides the API server and Web UI endpoint on port 8080. |
| `airflow-worker` | Executes tasks assigned through the executor (CeleryExecutor). |
| `airflow-triggerer` | Supports event-based waiting for deferrable tasks (not used in this practice). |
| `airflow-init` | First-time initialization: database migration and local user creation. |
| `postgres` | Stores Airflow metadata. |
| `redis` | Broker between scheduling and worker execution. |

> Key idea: you do not need to master the internal architecture yet. Recognize that
> orchestration requires parsing, scheduling, execution, metadata, and monitoring
> responsibilities.

> **Verified against the committed `docker-compose.yaml`:** the API server service is
> named `airflow-apiserver` (no hyphen between *api* and *server*), even though the
> course guide's table writes it as `airflow-api-server`. Use `airflow-apiserver` in
> `docker compose` commands, and note that the only exposed host port is `8080`.
>
> The Compose file mounts `dags/`, `logs/`, `config/`, and `plugins/` from the project
> root into the containers, which is why those four folders must exist before starting.

`.env` pins the image so every run uses the same release:

```
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.8
AIRFLOW_UID=50000
```

`AIRFLOW_UID` is the default Airflow container user; it suppresses the UID warning
that the Linux/WSL instructions refer to. On macOS it is harmless either way.

### Initialization outcome

Initialization must complete with **exit code 0**. It creates these
local development credentials:

| Field | Value |
| --- | --- |
| Username | `airflow` |
| Password | `airflow` |

> These credentials are suitable only for the local learning environment. They are
> not appropriate for production.

---

## 6. Access the Web UI

Open <http://localhost:8080> and log in with `airflow` / `airflow`.

### Pre-class checkpoint

- [ ] Docker Desktop running.
- [ ] Docker Compose 2.14.0 or newer verified.
- [ ] Airflow 3.1.8 Compose file present.
- [ ] Airflow initialization completed (exit code 0).
- [ ] Airflow containers running and healthy.
- [ ] Web UI accessible at <http://localhost:8080>.

If a container keeps restarting or stays unhealthy:

```bash
docker compose ps
docker compose logs --tail=100 airflow-init
docker compose logs --tail=100 airflow-apiserver
```

---

## 7. The DAG

`dags/reliable_pipeline_demo.py` implements the orchestration skeleton:

```
start → extract_data → validate_data → transform_data → load_data
```

| Task | Responsibility represented |
| --- | --- |
| `start` | Marks the beginning of the workflow. |
| `extract_data` | Obtaining source data. |
| `validate_data` | The raw-data quality gate (simulated). |
| `transform_data` | Cleaning, harmonization, conversion, integration, derivation. |
| `load_data` | Persistence of trusted data in a target. |

Concepts exercised:

| Concept | Meaning in this practice |
| --- | --- |
| Pipeline design | What should happen, which stages exist, what conditions permit progression. |
| Airflow DAG | How tasks and dependencies are orchestrated (the reusable blueprint). |
| DAG Run | One execution of the reusable workflow definition. |
| Task | A reusable unit of work. |
| Task Instance | The concrete execution of a task inside a specific DAG Run. |

DAG-level configuration:

| Parameter | Value | Meaning |
| --- | --- | --- |
| `dag_id` | `reliable_pipeline_demo` | Unique identifier used to find and manage the workflow. |
| `schedule` | `None` | No automatic schedule; every DAG Run is triggered manually. |
| `start_date` | `pendulum.datetime(2026, 1, 1, tz="UTC")` | Timezone-aware boundary. Does not trigger a run by itself when `schedule=None`. |
| `catchup` | `False` | Prevents historical scheduled runs; consistent with manual triggering. |
| `tags` | `["etl", "guided-practice"]` | Labels for filtering in the Web UI. |

> Do not execute the file with `python reliable_pipeline_demo.py`. Airflow discovers,
> displays, and executes it by parsing the mounted `dags/` directory.
>
> When a decorated `@task` function is called while the DAG is being defined, Airflow
> creates the task representation used in the workflow graph. It is not a normal
> immediate function call.
>
> Dependencies express **execution constraints**, not visual order. The tasks do not run
> in sequence merely because their definitions appear in that order in the file.

---

## 8. Run the three planned experiments

### Run 1 — Baseline (successful workflow)

1. Confirm `reliable_pipeline_demo` appears in the DAG list and is **unpaused**.
2. **Trigger DAG** (manual run button).
3. Open the new DAG Run and watch the task instances change state.
4. Wait for the final outcome.

Verify before triggering: the graph must contain **5 tasks** and **4 dependency edges**.

To confirm the structure locally, without Docker or the Web UI:

```bash
make verify-dag
```

Expected output (abridged):

```
  DAG parsing: OK -> dag_id=reliable_pipeline_demo
  catchup=False  tags=['etl', 'guided-practice']
  tasks (5): ... edges (4): ...
PASSED: 5 tasks and 4 edges match the design.
```

`scripts/verify_dag.py` only imports the DAG and inspects its graph; it never executes
tasks. It exits non-zero on a parse error or a task/edge count mismatch, which makes it a
fast pre-flight check before you start the stack.

### Inspect logs

1. Open the `validate_data` task instance in the successful run → open its log.
2. Locate:
   ```
   Validating incoming data...
   Raw data validation passed.
   ```
3. Repeat for `transform_data`:
   ```
   Transforming data...
   Transformation completed successfully.
   ```

Useful logs answer: *what ran, when, with what input/context, did it succeed, where and
why did it fail, what happened next.*

### Run 2 — Controlled failure experiment

Replace `validate_data` with the controlled failure:

```python
    @task
    def validate_data():
        print("Validating incoming data...")

        raise ValueError(
            "Controlled validation failure."
        )
```

1. Save the file; wait for the DAG processor to parse it.
2. Trigger a **new** DAG Run — do **not** clear or overwrite the successful run.
3. Observe each task instance.

Quality-gate intent:

```
Extract → Raw Validation → PASS?
                        ├─ YES → Transform
                        └─ NO  → Stop
```

A critical validation failure must prevent unsafe data from reaching transformation
and load. The operational policy decides whether the workflow stops, warns,
quarantines, or continues.

Then inspect the failed run: open the `validate_data` task instance, open its log, and
locate the exception message `Controlled validation failure.`

### Run 3 — Restore

Restore the original simulated `validate_data`, save, wait for parsing, trigger a third
DAG Run, and verify the dependency chain completes.

| Run | Purpose | Expected conceptual outcome |
| --- | --- | --- |
| 1 | Baseline | Successful workflow |
| 2 | Controlled experiment | Validation task fails; downstream behavior observed |
| 3 | Restoration | Successful workflow after validation is restored |

> This is a controlled **programming** failure. It is not real Great Expectations
> validation and does not represent automatic data cleaning.

---

## 9. Connection to Part 2

Part 1 built the orchestration skeleton. Part 2 evolves it to:

```
extract_data → validate_raw_data → transform_data → validate_prepared_data → load_data
```

| Stage | Future responsibility |
| --- | --- |
| Extract | Obtain actual source data. |
| Validate Raw | Execute a real Great Expectations quality contract against incoming data. |
| Transform | Clean, harmonize, integrate, convert, derive per known engineering rules. |
| Validate Prepared | Verify that transformation produced an acceptable result. |
| Load | Persist trusted data in the target. |

Reusable logic for those stages belongs in `src/`, not inside the DAG file.

| Component | Responsibility |
| --- | --- |
| ETL code | Extracts, cleans, transforms, integrates, and loads data. |
| Great Expectations | Evaluates data quality against explicit contracts. |
| Apache Airflow | Orchestrates when and in what order those operations run. |

> Airflow does not make ETL logic reliable by itself. It provides the orchestration
> mechanisms needed to execute, coordinate, monitor, and control that logic.

---

## 10. Reflection questions

1. What is the difference between a DAG and a DAG Run?
2. What is the difference between a Task and a Task Instance?
3. Why are dependencies more than simply the visual order of tasks?
4. Why did downstream tasks not proceed normally when `validate_data` failed?
5. Where would you investigate the cause of a failed task?
6. Why should a critical validation failure stop transformation?
7. Why would retrying a deterministic data quality problem usually not solve it?
8. Which responsibilities belong to Airflow and which to ETL or validation code?

Support answers with evidence from the graph, the three DAG Runs, task instance
states, and logs — not from memory.

---

## 11. Makefile shortcuts

```bash
make help                  # list all targets
make check                 # verify docker + compose versions and engine
make venv                  # create .venv (Python 3.12) + Task SDK with constraints
make verify-dag           # offline DAG structural check (no Docker needed)
make init                  # docker compose up airflow-init
make up / make down        # start / stop the stack
make ps                    # container status + health
make logs                  # follow logs from all services
make scheduler-version     # python version inside the containers
make dags-list             # DAGs Airflow currently sees
make dags-list-import-errors
make test-trigger          # trigger reliable_pipeline_demo from the CLI
make psql                  # psql shell on the metadata DB
make reset                 # DESTRUCTIVE: containers + volumes + logs
```

---

## 12. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `docker: command not found` | Docker Desktop not installed / not in PATH | Install Docker Desktop and start it. |
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Start Docker Desktop and wait for the engine. |
| DAG missing from the UI | Parse error, or parsing takes a few seconds | Wait ~10–30 s, then `make dags-list-import-errors`. |
| `ImportError: cannot import name 'dag' from 'airflow.sdk'` | `SQLAlchemy` resolved to an incompatible version | Reinstall the Task SDK **with** the constraints file (see §4). |
| `ModuleNotFoundError: airflow` in VS Code | Wrong interpreter | Select `.venv/bin/python` and install the Task SDK. |
| `cannot import name 'dag'` in VS Code terminal | Full `apache-airflow` installed locally | Remove it; only `apache-airflow-task-sdk` belongs in `.venv`. |
| `make: command not found` | Xcode Command Line Tools absent | `xcode-select --install`, or run the raw `docker compose` commands. |
| Web UI does not answer on 8080 | Services still starting or unhealthy | `make ps`, then `docker compose logs airflow-apiserver`. |
| Port 8080 already in use | Another process owns the port | Stop it, or change the host port in `docker-compose.yaml`. |
| Containers restart repeatedly | Insufficient memory / corrupted volume | Raise Docker memory, then `make reset`. |
| Task stuck in `queued` | Worker busy or unhealthy | `make ps` and check `airflow-worker` logs. |

Useful commands:

```bash
docker compose ps
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger reliable_pipeline_demo
docker compose down --volumes --remove-orphans   # full reset
```

---

## 13. Official technical references

- Apache Airflow. Running Airflow in Docker, version 3.1.8 —
  <https://airflow.apache.org/docs/apache-airflow/3.1.8/howto/docker-compose/index.html>
- Apache Airflow. Docker Compose configuration, version 3.1.8 —
  <https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml>
- Apache Airflow. Public Interface for Airflow 3.0+ —
  <https://airflow.apache.org/docs/apache-airflow/3.1.8/public-airflow-interface.html>
- Apache Airflow. Task SDK documentation and examples — <https://airflow.apache.org/docs/task-sdk/stable/>
- Docker. Docker Desktop for Mac — <https://docs.docker.com/desktop/setup/install/mac-install/>

Technical baseline verified for Apache Airflow 3.1.8. Course guide accessed September 2026.