__author__ = 'igorsf'

import os
import time
import json
import logging
import logging.config
import gzip
from common import settings

from datetime import datetime
from datetime import timedelta

import tempfile

import shutil

from azure.storage.blob import BlockBlobService

from client import AppNexusClient

logging.basicConfig(format='%(asctime)s %(name)-20s %(levelname)-5s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
logging.getLogger("client").setLevel(logging.INFO)

DSP_NAME = 'APPNEXUS'

class Pull:

    def __init__(self):

        self.api_client = AppNexusClient(
            settings.APPNEXUS_USERNAME,
            settings.APPNEXUS_PASSWORD
        )

        azure_account = settings.AZURE_ACCOUNTS[DSP_NAME]

        self.block_blob_service = BlockBlobService(
            account_name=azure_account['NAME'],
            account_key=azure_account['KEY']
        )

        self.pull_filename = settings.PULL_FILES[DSP_NAME]['LLD']

        # fetch 3 hours before last pulled timestamp
        self.pull_dt = None
        if os.path.exists(self.pull_filename):
            self.pull_dt = datetime.fromtimestamp(os.path.getmtime(self.pull_filename)) - timedelta(hours=3)

    def sync_log_level_data(self):

        log_files = self.get_available_logs()

        if log_files:
            self.download_logs(log_files)

        # update last pulled timestamp
        with open(self.pull_filename, 'a'):
            os.utime(self.pull_filename, None)

        logger.info("finished work")

    def get_available_logs(self):

        params = {'siphon_name': 'standard_feed'}

        if self.pull_dt:
            params['updated_since'] = self.pull_dt.strftime("%Y_%m_%d_%H")

        logger.debug("request params {0}".format(params))

        r = self.api_client.request('https://api.appnexus.com/siphon', params)

        resp = json.loads(r.content)["response"]

        logger.debug("response siphons {0}".format(resp))

        if resp.get("status", False) != "OK":
            return None

        return resp["siphons"]

    def build_file_name(self, log_name, log_hour, ts, part, extension):
        return log_name + "_" + log_hour + "_" + part + "." + extension

    def download_logs(self, log_files):

        counter = 0

        for log in log_files:
            for log_file in log["splits"]:
                filename = self.build_file_name(log["name"], log["hour"], log["timestamp"], log_file["part"], "gz")
                logger.debug("fetch filename {0}".format(filename))

                params = {'siphon_name': log["name"], 'hour': log['hour'], 'timestamp': log['timestamp'], 'split_part': log_file["part"] }
                r = self.api_client.request('https://api.appnexus.com/siphon-download', params, allow_redirects=False)

                counter += 1

                container_name = 'feeds'
                blob_name = 'standard_v2/' + filename

                file_path = self.download_file(r.url, filename)

                # Replace word "NULL" with empty value. Polybase can't convert 'NULL' for numeric columns
                self.replace(file_path, 'NULL', '')

                if file_path:
                    self.block_blob_service.create_blob_from_path(container_name, blob_name, file_path)
                    logger.debug("upload file {0}".format(file_path))
                    os.remove(file_path)
                    logger.debug("clean up file {0}".format(file_path))

        logger.info("processed {0} files".format(counter))

    def download_file(self, url, filename):
        r = self.api_client.request(url, stream=True)
        if r.status_code != 200:
            return False

        file_path = settings.TEMP_DIR + "/" + filename

        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()

        return file_path

    @staticmethod
    def replace(file_path, pattern, subst):
        out_file_fd, out_file_path = tempfile.mkstemp(suffix=".gz")
        with gzip.open(out_file_path, 'wb') as new_file:
            with gzip.open(file_path, 'rb') as old_file:
                for line in old_file:
                    new_file.write(line.replace(pattern, subst))
        os.remove(file_path)
        shutil.move(out_file_path, file_path)

if __name__ == '__main__':

    p = Pull()
    p.sync_log_level_data()

