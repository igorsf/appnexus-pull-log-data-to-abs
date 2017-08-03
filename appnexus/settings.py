__author__ = 'igorsf'

PULL_FILENAME = '.pull_lld'

PULL_SEGMENT_FILE = '.pull_segment'

# AppNexus credentials
APPNEXUS_USERNAME = '<appnexus_username>'
APPNEXUS_PASSWORD = '<appnexus_password>'

# azure credentials
AZURE_ACCOUNT_NAME = '<azure_account_name>'
AZURE_ACCOUNT_KEY = '<azure_account_key>'

# azure sql dw credentials
DATABASES = {
    'azure': {
        'DATABASE': '<database_name>',
        'USERNAME': '<username>',
        'PASSWORD': '<password>',
        'HOST': '<host>',
        'PORT': '1433',
        'DRIVER': '{ODBC Driver 13 for SQL Server}'
    },
}

# scratch temp space
TEMP_DIR = "/tmp"
