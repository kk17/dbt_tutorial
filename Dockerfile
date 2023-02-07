# https://github.com/orgs/dbt-labs/packages?visibility=public
FROM ghcr.io/dbt-labs/dbt-bigquery:1.4.0
ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
