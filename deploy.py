"""
deploy.py — Registra el deployment de market_data_flow en Prefect.
Corre una vez para registrar. Después Prefect lo dispara automáticamente.
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from market_data.orchestration.flows.batch_flow import market_data_flow

deployment = Deployment.build_from_flow(
    flow=market_data_flow,
    name="market-data-hourly",
    work_pool_name="local-pool",
    schedule=CronSchedule(cron="0 * * * *", timezone="UTC"),
    parameters={
        "env": "production",
        "config_dir": "/app/config",
    },
    tags=["market-data", "production"],
)

if __name__ == "__main__":
    deployment_id = deployment.apply()
    print(f"Deployment registrado: {deployment_id}")
