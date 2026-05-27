#!/bin/bash

# ---- Environment variables ----
# export PYTHONPATH=".."
# export CONFIG_FILE="config/test_config.json"
# export SSH_KEY_FILE=""
# export SSH_USER=""
# export DATABASE_FILE=""
# export HOST="0.0.0.0"
# export PORT="8000"

# ---- Run application ----
# `--host`: The host of the API. Default: 0.0.0.0
# `--port`: The port of the API. Default: 8000
# `--database-file`: The database file. Default: /db/db.sqlite3
# `--ssh-key-file`: The ssh key file. Default: System default
# `--ssh-user`: The ssh user. Default: metascheduler

export PYTHONPATH=".."
export SSH_BANNER_TIMEOUT=60
export SSH_TIMEOUT=30
export SSH_AUTH_TIMEOUT=30

echo "Select cgroups configuration:"
echo "1) best effort"
echo "2) shared"
echo "3) dynamic"
echo "4) exclusive"
read -rp "Option: " option

case "$option" in
    1)
        CONFIG_FILE="./config/config_cgroups_best_effort.json"
        ;;
    2)
        CONFIG_FILE="./config/config_cgroups_shared.json"
        ;;
    3)
        CONFIG_FILE="./config/config_cgroups_dynamic.json"
        ;;
    4)
        CONFIG_FILE="./config/config_cgroups_exclusive.json"
        ;;
    *)
        echo "Invalid option: $option"
        echo "Use 1, 2, 3, or 4."
        exit 1
        ;;
esac

echo "Starting application with config: $CONFIG_FILE"
echo "Starting application with pipenv..."

pipenv run python main.py "$CONFIG_FILE" \
    --host 0.0.0.0 \
    --port 8000 \
    --database-file ./db/db.sqlite3 \
    --ssh-key-file /home/gjaimejuan/.ssh/id_rsa \
    --ssh-user gjaimejuan
