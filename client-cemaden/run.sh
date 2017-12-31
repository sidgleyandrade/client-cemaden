#!/bin/bash -e

# Absolute path this script is in
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

if [ ! -d "$SCRIPTPATH/.virtual" ] && [ -r "$SCRIPTPATH/requirements.txt" ]; then
    echo "+++ Creating virtualenv +++"
    python -m virtualenv $SCRIPTPATH/.virtual

    source $SCRIPTPATH/.virtual/bin/activate
    echo "+++ Activating virtualenv +++"

    echo "+++ Upgrading pip and setuptools +++"
    pip install --upgrade pip
    pip install --upgrade setuptools

    echo "+++ Installing required packages +++"
    pip install -r $SCRIPTPATH/requirements.txt

    echo "+++ Deactivating virtualenv +++"
    deactivate
fi

if [ ! -f "$SCRIPTPATH/.virtual" ]; then
    echo "+++ Activating activate +++"
    source $SCRIPTPATH/.virtual/bin/activate

    echo "+++ Starting client-cemaden +++"
    python client-cemaden.py &
    echo "+++ Deactivating virtualenv +++"
    deactivate
fi
