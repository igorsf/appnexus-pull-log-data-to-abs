__author__ = 'fridman'

import os
import logging
import logging.config
from common import settings, utils
from datetime import datetime, timedelta

import boto3

from azure.storage.blob import BlockBlobService

logging.basicConfig(format='%(asctime)s %(name)-20s %(levelname)-5s %(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger("azure.storage").setLevel(logging.DEBUG)

logger = logging.getLogger('thetradedesk.reds')
logger.setLevel(logging.DEBUG)

#logger.debug('testing - debug')
#logger.info('testing - info')
#exit()

DSP_NAME = 'THETRADEDESK'

class Pull:

    def __init__(self):

        azure_account = settings.AZURE_ACCOUNTS[DSP_NAME]

        self.block_blob_service = BlockBlobService(
            account_name=azure_account['NAME'],
            account_key=azure_account['KEY']
        )

        self.pull_file = settings.PULL_FILES[DSP_NAME]['REDS']

        AWS_ACCOUNT = settings.AWS_ACCOUNTS[DSP_NAME]
        self.bucket_name = AWS_ACCOUNT['BUCKET_NAME']

        session = boto3.Session(
            aws_access_key_id=AWS_ACCOUNT['ACCESS_KEY'],
            aws_secret_access_key=AWS_ACCOUNT['SECRET_KEY']
        )

        s3 = session.resource("s3")
        self.bucket = s3.Bucket(self.bucket_name)

    def process(self):

        pull_dt = utils.load_date(
            self.pull_file,
            datetime(2017, 9, 26, 0, 0)  # default
        )

        dates = utils.date_range_hourly(pull_dt)
        for date in dates:
            prefix = 'pj95up2/redf5aggregated/date={}/hour={}/'.format(date.strftime('%Y-%m-%d'), date.hour)
            for object_summary in self.bucket.objects.filter(Prefix=prefix):
                obj = object_summary.Object()
                if obj.content_length > 0:
                    self.copy_s3file_to_azure(obj)

        utils.save_date(self.pull_file, (utils.latest_date(dates) - timedelta(hours=3)))

    def copy_s3file_to_azure(self, obj):

        # download and save local file
        tmp_filename = utils.extract_filename(obj.key)
        tmp_file_path = settings.TEMP_DIR + "/" + tmp_filename
        logger.info("Downloading s3 obj %s to %s", obj.key, tmp_file_path)
        obj.download_file(tmp_file_path)
        logger.debug("Finished download s3 obj %s", obj.key)

        # upload to azure blob storage
        folder = utils.extract_log_type(tmp_filename)
        if folder is None:
            logger.error("Unable to identify log type from file {}".format(tmp_filename))
            return

        blob_name = folder + '/' + tmp_filename
        logger.info("Uploading file {} to azure".format(blob_name))
        self.block_blob_service.create_blob_from_path('reds', blob_name, tmp_file_path)
        logger.debug("Finished upload file {0}".format(tmp_file_path))
        os.remove(tmp_file_path)
        logger.debug("Removed file {0}".format(tmp_file_path))

if __name__ == '__main__':
    p = Pull()
    p.process()
