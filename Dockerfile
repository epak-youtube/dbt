FROM python:3.10-slim-buster

WORKDIR /usr/src/dbt

RUN pip install dbt-core==1.9.0
RUN pip install dbt-snowflake==1.9.0

# Copy dbt files
COPY profiles.yml profiles.yml
COPY dbt_project.yml dbt_project.yml
COPY models models
COPY seeds seeds
COPY snapshots snapshots
COPY macros macros

RUN dbt deps

COPY dbt_packages dbt_packages

CMD ["tail", "-f", "/dev/null"]