from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/S-Gunjan/BA882-Team02-project.git",
        entrypoint="/home/gunjan21/BA882-Team02-project/flows/elt.py:elt_flow",
    ).deploy(
        name="311-Service-Requests-elt",
        work_pool_name="gunjan-pool1",
        job_variables={"env": {"GUNJAN": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="15 0 * * *",
        tags=["prod"],
        description="The pipeline to populate the stage schema with the newest posts.  Version is just for illustration",
        version="1.0.0",
    )