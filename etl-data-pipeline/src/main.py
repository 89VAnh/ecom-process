from datalake.base.enums.env import Env
from datalake.core.factory.usecase import USFactory

if __name__ == "__main__":
    # USFactory.run(env=Env.DEV, job_name="raw_to_silver")
    # USFactory.run(env=Env.DEV, job_name="build_fact_products")
    USFactory.run(env=Env.DEV, job_name="build_fact_histories")
    # USFactory.run(env=Env.DEV, job_name="fact_products_to_db")
    USFactory.run(env=Env.DEV, job_name="fact_histories_to_db")
