#!/bin/bash

if [ "$CONDA_DEFAULT_ENV" == "proda" ] || [ "$CONDA_DEFAULT_ENV" == "deva" ]; then
    echo "Shutting down Connect API";
    killall -SIGINT gunicorn;
    sleep 3;
    if [ `ps -e | grep -c gunicorn` -gt 0 ]; then
        echo "Connect API still running";
    else
        echo "Connect API terminated";
    fi
elif [ "$CONDA_DEFAULT_ENV" == "prodp" ] || [ "$CONDA_DEFAULT_ENV" == "devp" ]; then
    echo "Shutting down Connect Processor";
    kill -SIGINT `cat pid.log`;
    sleep 3;
    while [ $(ps -e | grep -c $(cat pid.log)) -gt 0 ]; do
        echo "Connect Processor still running";
        sleep 3;
    done
    echo "Connect Processor shut down";
fi

