Welcome to my personal dbt project regarding stocks. The goal of this project is to use as many tools from the data engineering toolkit as possible. Unfortunately, degiro has no API. Therefore, stock transactions are imported with a static csv downloadable from the degiro platform. These are hidden because of sensitive information. 

Current tools:
- dbt
- docker
- metabase
- postgres
- using yfinance api to retrieve stock data

Tools / features to come:
- cicd with github actions
- terraform
- aws ec2 for hosting metabase possibly
- sandbox / production env
- airbyte

How to fire up the docker containers: `docker compose up`

How to start a virtual env: `source env/bin/activate`

How to start up the proper postgres db: `psql -h localhost -p 5433 -U postgres -d destination_db`
