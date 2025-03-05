FROM python:3.10-slim-buster

WORKDIR /usr/app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ssh \
    && rm -rf /var/lib/apt/lists/*

RUN pip install dbt-core==1.9.0
RUN pip install dbt-snowflake==1.9.0

# Copy dbt project files
COPY . /usr/app/dbt/

# Copy entrypoint script
COPY entrypoint.sh /usr/app/entrypoint.sh
RUN chmod +x /usr/app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/usr/app/entrypoint.sh"]

CMD ["tail", "-f", "/dev/null"]