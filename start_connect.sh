#!/bin/bash

EXIT_LOG_LINES=30
GUNICORN_WORKERS=5
GUNICORN_TIMEOUT=31

if [ "$CONDA_DEFAULT_ENV" == "proda" ]; then
    echo "Starting Connect API for Production";
    export FLASK_ENV=production;
    rm exit.log;
    touch proda.log;
    truncate --size 0 proda.log;
    nohup gunicorn --bind 127.0.0.1:5000 --timeout $GUNICORN_TIMEOUT -w $GUNICORN_WORKERS \
        --graceful-timeout $(($GUNICORN_TIMEOUT * 2)) --log-level info \
        mdf_connect_server.api.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    sleep 3;
    if [ `cat exit.log` == `cat /dev/null` ]; then
        echo "Connect Prod API started.";
    else
        echo "Error starting Prod API:\n\n$(cat exit.log)";
    fi

elif [ "$CONDA_DEFAULT_ENV" == "deva" ]; then
    echo "Starting Connect API for Development";
    export FLASK_ENV=development;
    rm exit.log;
    touch deva.log;
    truncate --size 0 deva.log;
    nohup gunicorn --bind 127.0.0.1:5000 --timeout $GUNICORN_TIMEOUT -w $GUNICORN_WORKERS \
        --graceful-timeout $(($GUNICORN_TIMEOUT * 2)) --log-level debug \
        mdf_connect_server.api.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    sleep 5;
    if [ `cat exit.log` == `cat /dev/null` ]; then
        echo "Connect Dev API started.";
    else
        echo "Error starting Dev API:\n\n$(cat exit.log)";
    fi

elif [ "$CONDA_DEFAULT_ENV" == "prodp" ]; then
    if [ -f pid.log ]; then
        echo "Connect Processing already started!"
    else
        echo "Starting Connect Processing for Production";
        export FLASK_ENV=production;
        rm exit.log;
        touch prodp.log;
        truncate --size 0 prodp.log;
        nohup python3 -c "from mdf_connect_server.processor import processor; processor()" \
            | tail -n $EXIT_LOG_LINES &>exit.log & disown;
        sleep 5;
        if [ `cat exit.log` == `cat /dev/null` ]; then
            echo "Connect Prod Processor started.";
        else
            echo "Error starting Prod Processor:\n\n$(cat exit.log)";
        fi
    fi

elif [ "$CONDA_DEFAULT_ENV" == "devp" ]; then
    if [ -f pid.log ]; then
        echo "Connect Processing already started!"
    else
        echo "Starting Connect Processing for Development";
        export FLASK_ENV=development;
        rm exit.log;
        touch devp.log;
        truncate --size 0 devp.log;
        nohup python3 -c "from mdf_connect_server.processor import processor; processor()" \
            | tail -n $EXIT_LOG_LINES &>exit.log & disown;
        sleep 5;
        if [ `cat exit.log` == `cat /dev/null` ]; then
            echo "Connect Dev Processor started.";
        else
            echo "Error starting Dev Processor:\n\n$(cat exit.log)";
        fi
    fi

else
    echo "CONDA_DEFAULT_ENV '$CONDA_DEFAULT_ENV' invalid!";
fi
