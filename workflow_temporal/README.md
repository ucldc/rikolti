# Run Rikolti prototype workflow using Temporal

## Run a temporal dev server
Follow the instruction on this page to run a temporal development server on your local machine: [https://docs.temporal.io/application-development/foundations?lang=python#run-a-development-server](https://docs.temporal.io/application-development/foundations?lang=python#run-a-development-server)

The Temporal Server should be available on `localhost:7233` and the Temporal Web UI should be accessible at [http://localhost:8233]().

## Install the temporalio Python SDK

Set up a venv and install the `temporalio` python SDK and other requirements into it.

```
cd workflow_temporal
python3 -m venv .env
source ./.env/bin/activate
pip install -r requirements
pip install -r ../metadata_fetcher/requirements.txt
```

## Run a temporal worker

``` 
python run_worker.py 
```

## Run a workflow

Run all harvesting steps for collection 12345:

```
python run_workflow.py 12345
```

Run only steps 1 and 2 (fetching and mapping) for collection 12345:

```
python run_workflow.py 12345 --tasks 12
```

