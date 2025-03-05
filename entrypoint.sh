#!/bin/bash
set -e

# Change to the dbt project directory
cd /usr/app/dbt

# Install dependencies
dbt deps

# Execute the command passed to the container
if [ "$1" = "run" ]; then
    echo "Running dbt models..."
    dbt run
elif [ "$1" = "test" ]; then
    echo "Testing dbt models..."
    dbt test
elif [ "$1" = "build" ]; then
    echo "Building dbt models..."
    dbt build
else
    # If no command specified or custom command
    echo "No valid command specified. Running dbt with arguments: $@"
fi

# Capture exit code
exit_code=$?
echo "dbt command completed with exit code: $exit_code"
exit $exit_code