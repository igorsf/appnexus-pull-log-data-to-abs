
This script pulls "log level data" files from AppNexus to Azure Blob Storage

# Instructions

To run

    python -m appnexus.logleveldata 

To install source folder in your python env

    python setup.py develop

To run the test, simply invoke your favorite test runner, or execute a test file directly; any of the following work:

    python -m unittest discover
    python test/test.py
    nosetests
    
To build deb package
    
    python setup.py --command-packages=stdeb.command bdist_deb
