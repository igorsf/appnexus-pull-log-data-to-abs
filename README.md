# appnexus-pull-log-data-to-abs

Pull AppNexus Log Level Data to Azure Blob Storage

# DEVELOPER INSTRUCTIONS

To run

    python -m appnexus.pull 

To install source folder in your python env

    python setup.py develop

To run the test, simply invoke your favorite test runner, or execute a test file directly; any of the following work:

    python -m unittest discover
    python test/test.py
    nosetests
    
To build deb package
    
    python setup.py --command-packages=stdeb.command bdist_deb
