__author__ = 'igorsf'

import os
import settings
import json
import logging
import logging.config
import pyodbc

from datetime import datetime
from datetime import timedelta

from client import AppNexusClient

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
logging.getLogger("client").setLevel(logging.INFO)

class SyncCampaignSegments:

    def __init__(self):

        self.api_client = AppNexusClient(
            settings.APPNEXUS_USERNAME,
            settings.APPNEXUS_PASSWORD
        )

        db = settings.DATABASES['azure']

        self.cnxn = pyodbc.connect('DRIVER='+ db['DRIVER']+';PORT='+ db['PORT']+';SERVER='
                                   +db['HOST']+';PORT='+db['PORT']+';DATABASE='+db['DATABASE']
                                   +';UID='+db['USERNAME']+';PWD='+ db['PASSWORD'])

        self.pull_filename = settings.PULL_SEGMENT_FILE

        # fetch 3 hours before last pulled timestamp
        self.pull_dt = None
        if os.path.exists(self.pull_filename):
            self.pull_dt = datetime.fromtimestamp(os.path.getmtime(self.pull_filename)) - timedelta(hours=3)

    def pull_profile_segments(self, profile_id):

        params = {'id': profile_id}

        r = self.api_client.request('https://api.appnexus.com/profile', params)

        resp = json.loads(r.content)["response"]

        if resp.get("status", False) != "OK":
            return None

        segment_group_target = resp["profile"]["segment_group_targets"]

        segments = []

        if segment_group_target is None:
            return segments

        for segment_group in segment_group_target:
            for segment in segment_group["segments"]:
                segments.append({
                    "id": segment['id'],
                    "name": segment["name"],
                    "action": segment['action']
                })

        return segments

    def pull_campaigns(self):

        params = {}
        if self.pull_dt:
            params['min_last_modified'] = self.pull_dt.strftime("%Y-%m-%d+%H:%M:%S")

        r = self.api_client.request('https://api.appnexus.com/campaign', params)

        resp = json.loads(r.content)["response"]

        if resp.get("status", False) != "OK":
            return None

        return resp["campaigns"]

    def sync_segments(self):

        for campaign in self.pull_campaigns():
            with self.cnxn.cursor() as cursor:
                cursor.execute("DELETE FROM [CampaignSegment] WHERE campaign_id = ?", campaign['id'])
                segments = self.pull_profile_segments(campaign["profile_id"])

                params = []
                for segment in segments:
                    params.append( (campaign['id'], campaign['profile_id'], segment['id'], segment['name'], segment['action']))

                if params:
                    cursor.executemany(
                        "INSERT INTO [CampaignSegment] (campaign_id, profile_id, segment_id, segment_name, "
                        "segment_action) values (?, ?, ?, ?, ?)", params)

        # update last pulled timestamp
        with open(self.pull_filename, 'a'):
            os.utime(self.pull_filename, None)

        logger.info("finished updating campaign segments")

if __name__ == '__main__':

    p = SyncCampaignSegments()
    p.sync_segments()