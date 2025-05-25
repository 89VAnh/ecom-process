from fastapi import FastAPI, HTTPException
from datalake.base.enums.env import Env
from datalake.core.factory.usecase import USFactory
import uvicorn

app = FastAPI(title="Data Lake Job Runner API")

# List of jobs to run in sequence
JOB_SEQUENCE = [
    "raw_to_silver",
    "build_fact_products",
    "build_fact_histories",
    "fact_products_to_db",
    "fact_histories_to_db",
]


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/run-etl-jobs")
async def run_job_sequence():
    results = []
    try:
        for job_name in JOB_SEQUENCE:
            try:
                USFactory.run(env=Env.DEV, job_name=job_name)
                results.append(
                    {"job_name": job_name, "status": "success", "env": "DEV"}
                )
            except Exception as e:
                results.append(
                    {"job_name": job_name, "status": "failed", "error": str(e)}
                )
        return {"status": "completed", "jobs": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job sequence failed: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8010)
