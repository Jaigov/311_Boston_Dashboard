from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/S-Gunjan/BA882-Team02-project.git",
        entrypoint="/home/gunjan21/BA882-Team02-project/prefect/flows/ml-views.py:ml_datasets",
    ).deploy(
        name="ml_datasets",
        work_pool_name="gunjan-pool1",
        job_variables={"env": {"GROUP2": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="20 0 * * *",
        tags=["prod"],
        description="The pipeline to create ML datasets off of the staged data.  Version is just for illustration",
        version="1.0.0",
    )