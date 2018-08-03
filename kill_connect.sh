#!/bin/bash


if [ "$CONDA_DEFAULT_ENV" == "proda" || "$CONDA_DEFAULT_ENV" == "deva"]; then
    echo "Killing Connect API"
    killall -SIGKILL gunicorn
    sleep 1
    if [ $("ps -e | grep -c gunicorn") -gt 0 ]; then
        echo "Connect API remains alive"
    else
        echo "Connect API killed"
    fi
elif [ "$CONDA_DEFAULT_ENV" == "prodp" || "$CONDA_DEFAULT_ENV" == "devp"]; then
    echo "Killing Connect Processor"
    killall -SIGKILL python
    sleep 1
    if [ $("ps -e | grep -c python") -gt 0 ]; then
        echo "Connect Processor remains alive"
    else
        echo "Connect Processor killed"
    fi
fi

