#!/bin/bash

EXIT_LOG_LINES=30

if [ "$CONDA_DEFAULT_ENV" == "proda" ]; then
    echo "Starting Connect API for Production";
    export FLASK_ENV=production;
    rm exit.out;
    touch proda.log;
    truncate --size 0 proda.log;
    nohup gunicorn --bind 127.0.0.1:5000 --timeout 61 -w 5 --log-level info \
        mdf_connect_server.api.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    echo "Connect Prod API started."
elif [ "$CONDA_DEFAULT_ENV" == "deva" ]; then
    echo "Starting Connect API for Development";
    export FLASK_ENV=development;
    rm exit.out;
    touch deva.log;
    truncate --size 0 deva.log;
    nohup gunicorn --bind 127.0.0.1:5000 --timeout 61 -w 5 --log-level debug \
        mdf_connect_server.api.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    echo "Connect Dev API started."
elif [ "$CONDA_DEFAULT_ENV" == "prodp" ]; then
    echo "Starting Connect Processing for Production";
    export FLASK_ENV=production;
    rm exit.out;
    touch prodp.log;
    truncate --size 0 prodp.log;
    nohup python3 -c "from mdf_connect_server.processor import processor; processor()" \
        | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    echo "Connect Prod Processor started"
elif [ "$CONDA_DEFAULT_ENV" == "devp" ]; then
    echo "Starting Connect Processing for Development";
    export FLASK_ENV=development;
    rm exit.out;
    touch devp.log;
    truncate --size 0 devp.log;
    nohup python3 -c "from mdf_connect_server.processor import processor; processor()" \
        | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    echo "Connect Dev Processor started"
else
    echo "CONDA_DEFAULT_ENV '$CONDA_DEFAULT_ENV' invalid!";
fi
