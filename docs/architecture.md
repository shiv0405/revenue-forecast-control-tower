# Architecture

## Overview

Revenue Forecast Control Tower is structured as a mixed-stack analytics application:

- FastAPI serves forecast and alert endpoints to operational consumers
- PostgreSQL holds dimensions, facts, analytical views, and executive KPI aggregations
- Lambda functions normalize inbound operational signals and prepare alert payloads
- TypeScript renders a browser dashboard from exported snapshot data
- Python orchestration generates data, computes forecast outputs, and writes artifacts

## Application Layers

- `src/revenue_forecast_control_tower/data_generation.py`
  Creates realistic multi-region bookings, pipeline, product usage, and contract datasets
- `src/revenue_forecast_control_tower/service.py`
  Computes KPIs, risk views, scenarios, and dashboard snapshot exports
- `src/revenue_forecast_control_tower/api.py`
  Exposes the service outputs through FastAPI
- `src/revenue_forecast_control_tower/repository.py`
  Provides a file-backed repository that can later be replaced by a database-backed adapter

## Data Flow

1. Synthetic source data is generated for accounts, opportunities, bookings, renewals, and usage.
2. The service layer computes regional forecast attainment, pipeline quality, account-level risk, and alert conditions.
3. Curated outputs are exported to JSON and CSV for dashboard and reporting use.
4. Lambda functions demonstrate how incremental events could be validated and routed into the warehouse pipeline.

## Deployment Shape

- PostgreSQL can be run locally via Docker Compose
- FastAPI can be hosted behind ALB, API Gateway, ECS, or Lambda Web Adapter
- Dashboard assets can be published to S3 and CloudFront or a standard static web host
- Lambda handlers can be triggered by EventBridge, S3 notifications, or API Gateway integrations
