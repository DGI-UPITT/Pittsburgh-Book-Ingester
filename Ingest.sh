#!/bin/bash

#PYTHON=/usr/lib/python2.4/bin/python2.4
PYTHON=python
# script is running as root
if [ $EUID -ne 0 ]; then
    echo "This script requires root access to run"
else
    # pass script parameters to the python script
    ${PYTHON} Ingester.py $@ > >(tee stdout.log) 2>stderr.log
fi
