from prefect import flow
from prefect.events import DeploymentEventTrigger

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/S-Gunjan/BA882-Team02-project.git",
        entrypoint="/home/gunjan21/BA882-Team02-project/ml/pipeline/flows/fit-model.py:training_flow",
    ).deploy(
        name="mlops-train-model",
        work_pool_name="gunjan-pool1",
        job_variables={"env": {"GUNJAN": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        tags=["prod"],
        description="Pipeline to train a model and log metrics and parameters for a training job",
        version="1.0.0",
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "ml-datasets"}
            )
        ]
    )