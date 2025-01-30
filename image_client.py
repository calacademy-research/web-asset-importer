import json

import requests, hmac
import time
import sys
import server_host_settings
from uuid import uuid4
import datetime
import logging
import os
from monitoring_tools import MonitoringTools
from datetime import datetime, timezone, timedelta
from urllib.parse import quote


TIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"


class UploadFailureException(Exception):
    pass


class DuplicateImageException(Exception):
    pass


class DeleteFailureException(Exception):
    pass


class FileNotFoundException(Exception):
    pass


class ImageClient:
    def __init__(self, config=None):
        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')

        self.ptc_timezone = timezone(timedelta(hours=-8), name="PST")
        self.update_time_delta()
        self.config = config

        if config.MAILING_LIST:
            if hasattr(config, 'ACTIVE_REPORT_PATH'):
                report_path = config.ACTIVE_REPORT_PATH
                active = True
            else:
                report_path = config.REPORT_PATH
                active = False

            # dict to create report of image import
            self.monitoring_dict = {}

            self.monitoring_tools = MonitoringTools(config=config, report_path=report_path, active=active)


    def split_filepath(self, filepath):
        cur_filename = os.path.basename(filepath)
        cur_file_ext = cur_filename.split(".")[-1]
        return cur_filename, cur_file_ext

    def build_url(self, endpoint):
        host = server_host_settings.SERVER_NAME
        port = server_host_settings.SERVER_PORT
        prefix = server_host_settings.SERVER_PREFIX
        return f"{prefix}://{host}:{port}/{endpoint}"

    def update_time_delta_from_response(self, response):
        global server_time_delta
        try:
            timestamp = response.headers['X-Timestamp']
        except KeyError:
            server_time_delta = 0
            return

        server_time_delta = int(timestamp) - int(time.time())
        print(f"Updated server time delta to {server_time_delta}")

    def get_timestamp(self):
        """Return an integer timestamp with one second resolution for
        the current moment.
        """

        return int(time.time()) + server_time_delta

    def update_time_delta(self):
        response = requests.get(self.build_url(""))
        self.update_time_delta_from_response(response)

    def generate_token(self, filename):
        """Generate the auth token for the given filename and timestamp. """
        timestamp = self.get_timestamp()
        # print(f"image client timestamp: {timestamp}", flush=True)
        msg = str(timestamp).encode() + filename.encode()
        mac = hmac.new(server_host_settings.SERVER_KEY.encode(), msg=msg, digestmod='md5')
        return ':'.join((mac.hexdigest(), str(timestamp)))

    def delete_from_image_server(self, attach_loc, collection):
        data = {
            'filename': attach_loc,
            'coll': collection,
            'token': self.generate_token(attach_loc),
        }
        url = self.build_url("filedelete")
        self.logger.debug(f"Deleting {url} from server")
        r = requests.post(url, data=data)
        if r.status_code == 404:
            raise FileNotFoundException
        if r.status_code != 200:
            print(f"Deletion failed, aborted: {r.status_code}:{r.text}")
            raise DeleteFailureException

    def get_internal_filename(self, original_path, collection):
        """Retrieve the internal filename from the server based on the original file path."""
        params = {
            'file_string': quote(original_path),
            'coll': collection,
            'search_type': 'path',  # Query by file path
            'token': self.generate_token(quote(original_path))
        }

        url = self.build_url("getImageRecord")
        r = requests.get(url, params=params)

        if r.status_code == 404:
            self.logger.warning(f"No record found for path {original_path} in collection {collection}.")
            return None
        elif r.status_code == 200:
            response_data = r.json()
            if isinstance(response_data, list) and len(response_data) > 0:
                internal_filename = response_data[0].get("internal_filename")  # Adjust key if necessary
                self.logger.debug(f"Found internal filename for {original_path}: {internal_filename}")
                return internal_filename

        self.logger.error(f"Failed to retrieve internal filename for {original_path}. Status: {r.status_code}")
        return None

    def upload_to_image_server(self, full_path, redacted, collection, original_path=None, id=None):
        if full_path is None or redacted is None or collection is None:
            errstring = f"Bad input failures to upload to image server: {full_path} {redacted} {collection}"
            print(errstring, file=sys.stderr, flush=True)
            raise UploadFailureException(errstring)
        datetime_now = datetime.now(self.ptc_timezone)
        local_filename = full_path
        uuid = str(uuid4())
        extension = local_filename.split(".")[-1]
        attach_loc = uuid + "." + extension
        if original_path is not None:
            upload_path = original_path
        else:
            upload_path = full_path
        data = {
            'store': attach_loc,
            'type': 'image',
            'coll': collection,
            'token': self.generate_token(attach_loc),
            'original_filename': os.path.basename(local_filename),
            'original_path': upload_path,
            'redacted': str(redacted),
            'notes': None,
            'datetime': datetime_now.strftime(TIME_FORMAT)
        }

        files = {
            'image': (attach_loc, open(local_filename, 'rb')),
        }
        url = self.build_url("fileupload")
        self.logger.debug(f"Attempting upload of local converted file {local_filename} to {url}")
        r = requests.post(url, files=files, data=data)

        if id is None:
            id = 'N/A'
        if r.status_code != 200:
            self.logger.error(f"FAIL - return code {r.status_code}. data: {data}")
            if r.status_code == 409:
                self.logger.error(f"Image already in server; skipping for {upload_path}")
                raise DuplicateImageException
            else:
                self.logger.error(f"Image upload aborted: {r.status_code}:{r.text}")

            if self.config.MAILING_LIST:
                self.monitoring_dict = self.monitoring_tools.append_monitoring_dict(self.monitoring_dict, id,
                                                                                    original_path, False)

            raise UploadFailureException
        else:
            params = {
                'filename': attach_loc,
                'coll': collection,
                'type': 'image',
                'token': self.generate_token(attach_loc)
            }

            r = requests.get(self.build_url("getfileref"), params=params)
            url = r.text
            assert r.status_code == 200

            logging.info(f"Uploaded: {local_filename},{attach_loc},{url}")
            logging.info("adding to image")
            if self.config.MAILING_LIST:
                self.monitoring_dict = self.monitoring_tools.append_monitoring_dict(self.monitoring_dict, id,
                                                                                    original_path, True)

        self.logger.debug("Upload to file server complete")

        return url, attach_loc

    # works for just basename +ext. "exact" does a sql "like" operation
    def check_image_db_if_filename_imported(self, collection, filename, exact=False):
        params = {
            'file_string': quote(filename),
            'coll': collection,
            'exact': exact,
            'search_type': 'filename',
            'token': self.generate_token(quote(filename))
        }

        return self.decode_response(params)

    def write_exif_image_metadata(self, exif_dict, collection, filename):

        data = {'filename': filename,
                'coll': collection,
                'exif_dict': json.dumps(exif_dict),
                'token': self.generate_token(filename)
                }

        url = self.build_url('updateexifdata')

        response = requests.post(url, data=data)

        if response.status_code == 200:
            self.logger.debug(f"EXIF data for '{filename}' updated successfully.")
        else:
            self.logger.error(
                f"Failed to update EXIF data for '{filename}': {response.status_code} - {response.text}")

        self.logger.debug(f"modifying EXIF data for {filename} complete")

    def read_exif_image_data(self, collection, filename, datatype):
        params = {'filename': filename,
                  'coll': collection,
                  'dt': datatype,
                  'search_type': 'filename',
                  'token': self.generate_token(filename)
                  }
        url = self.build_url("getexifdata")

        response = requests.get(url=url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            return None

    import time
    import requests

    def decode_response(self, params):
        url = self.build_url("getImageRecord")
        max_duration = 10800  # 3 hours in seconds
        start_time = time.time()
        last_status_code = None
        last_error = None
        delay = 0

        while time.time() - start_time < max_duration:
            try:
                r = requests.get(url, params=params)
                last_status_code = r.status_code  # Store last response code

                if r.status_code == 200:
                    self.logger.debug(f"Checked {params['file_string']} - already imported")
                    return True  # Success

                if r.status_code == 404:
                    self.logger.error(f"Checked {params['file_string']} - No duplicates found (404)")

                if r.status_code == 500:
                    self.logger.error(f"500: Internal server error checking {params['file_string']}")
                    self.logger.error(f"URL: {url}")

            except requests.RequestException as e:
                last_error = str(e)  # Store last exception message
                self.logger.error(f"Request failed: {e}")

            self.logger.debug(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay += 30

        # If we exit the loop, all attempts have failed within 3 hours
        error_message = (
            f"Failed to get a valid response for {params['file_string']} within 3 hours. "
            f"Last status code: {last_status_code}, Last error: {last_error}"
        )
        raise RuntimeError(error_message)

