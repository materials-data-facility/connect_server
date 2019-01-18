#!/bin/bash

if [ "$CONDA_DEFAULT_ENV" == "proda" ] || [ "$CONDA_DEFAULT_ENV" == "deva" ]; then
    echo "Shutting down Connect API";
    killall -SIGTERM gunicorn;
    sleep 3;
    if [ `ps -e | grep -c gunicorn` -gt 0 ]; then
        echo "Connect API still running";
    else
        echo "Connect API terminated";
    fi
elif [ "$CONDA_DEFAULT_ENV" == "prodp" ] || [ "$CONDA_DEFAULT_ENV" == "devp" ]; then
    echo "Shutting down Connect Processor";
    if [ -f pid.log ]; then
        kill -SIGTERM `cat pid.log`;
        sleep 10;
        while [ $(ps -e | grep -c $(cat pid.log)) -gt 0 ]; do
            echo "Connect Processor still running";
            sleep 60;
        done
        rm pid.log
        echo "Connect Processor shut down";
    else
        echo "Connect Processor not running";
    fi
fi

