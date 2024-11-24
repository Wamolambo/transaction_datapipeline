#!/bin/bash

echo "Installing Airflow"
# 1. Install Airflow and its dependencies
echo "Install dependencies"
pip install -r requirements.txt

# 2. Set the Airflow home directory
echo "Configuring Airflow Home"
export AIRFLOW_HOME=/root/transaction_data_workflow
echo "AIRFLOW_HOME is set to: $AIRFLOW_HOME"


# 3. Initialize the Airflow database
echo "Initializing Airflow database..."
airflow db init

# 4. Create an Airflow user (only needed for Role-Based Access Control [RBAC])
echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin_password

# 5. Start the Airflow scheduler (in the background)
echo "Starting Airflow scheduler..."
airflow scheduler -D

# 6. Start the Airflow webserver (in the background)
echo "Starting Airflow webserver..."
airflow webserver

echo "Airflow setup is complete. Webserver is running at http://localhost:8080"