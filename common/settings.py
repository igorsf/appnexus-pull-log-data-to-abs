__author__ = 'igorsf'

PULL_FILES = {
    'APPNEXUS': {
        'LLD': '.pull_lld',
        'SEGMENT': '.pull_segment'
    },
    'THETRADEDESK': {
        'REDS': '.pull_reds'
    }
}


# AppNexus credentials
APPNEXUS_USERNAME = '<appnexus_username>'
APPNEXUS_PASSWORD = '<appnexus_password>'

# thetradedesk s3 credentials
AWS_ACCOUNTS = {
    'THETRADEDESK': {
        'ACCESS_KEY':'<access_key>',
        'SECRET_KEY':'<secret_key>',
        'BUCKET_NAME': '<bucket_name>',
        'REDS_PATH': '<reds_path>'
    },
}

# metadata azure blog storage
AZURE_ACCOUNTS = {
    'APPNEXUS': {
        'NAME': '<account_name>',
        'KEY': '<account_key>',
    },
    'THETRADEDESK': {
        'NAME': '<account_name>',
        'KEY': '<account_key>',
        'CONTAINER_NAME': 'reds'
    }
}

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
