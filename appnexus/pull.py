__author__ = 'igorsf'

import os
import time
import settings
import json
import logging
import logging.config

from azure.storage.blob import BlockBlobService

from client import AppNexusClient

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
logging.getLogger("client").setLevel(logging.INFO)

class Pull:

    def __init__(self):

        self.api_client = AppNexusClient(
            settings.APPNEXUS_USERNAME,
            settings.APPNEXUS_PASSWORD
        )

        self.block_blob_service = BlockBlobService(
            account_name=settings.AZURE_ACCOUNT_NAME,
            account_key=settings.AZURE_ACCOUNT_KEY
        )

        self.pull_filename = settings.PULL_FILENAME

        # fetch last pull timestamp
        self.pull_timestamp = None
        if os.path.exists(self.pull_filename):
            self.pull_timestamp = time.gmtime(os.path.getmtime(self.pull_filename))

    def do_work(self):

        log_files = self.get_available_logs()

        if log_files:
            self.download_logs(log_files)

        # update last pulled timestamp
        with open(self.pull_filename, 'a'):
            os.utime(self.pull_filename, None)

    def get_available_logs(self):

        params = {'siphon_name': 'standard_feed'}

        if self.pull_timestamp:
            params['updated_since'] = time.strftime("%Y_%m_%d_%H", self.pull_timestamp)

        logger.debug("request params {0}".format(params))

        r = self.api_client.request('https://api.appnexus.com/siphon', params)

        resp = json.loads(r.content)["response"]

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
                blob_name = 'standard/' + filename

                file_path = self.download_file(r.url, filename)

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

if __name__ == '__main__':

    p = Pull()
    p.do_work()

