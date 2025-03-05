FROM python:3.10-slim-buster

WORKDIR /usr/app

# Use shell form for more flexible script handling
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

RUN pip install dbt-core==1.9.0
RUN pip install dbt-snowflake==1.9.0

# Copy dbt project files
COPY . /usr/app/dbt/

# Copy entrypoint script
COPY --chmod=755 entrypoint.sh /usr/app/entrypoint.sh
RUN dos2unix /usr/app/entrypoint.sh && \
    chmod +x /usr/app/entrypoint.sh && \
    ls -l /usr/app/entrypoint.sh  # List file details to verify

# Set the entrypoint
ENTRYPOINT ["/bin/bash", "/usr/app/entrypoint.sh"]

CMD ["tail", "-f", "/dev/null"]