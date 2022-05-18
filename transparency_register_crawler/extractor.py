import logging
import os
from shutil import copyfileobj
from urllib.request import Request, urlopen
from pathlib import Path

from build.gen.bakdata.corporate.v1.corporate_pb2 import Person  # type: ignore
from build.gen.bakdata.corporate.v1.corporate_pb2 import Organization  # type: ignore
log = logging.getLogger(__name__)


class TransparencyRegisterExtractor:
    DOWNLOAD_URL = "http://ec.europa.eu/transparencyregister/public/consultation/statistics.do?action=getLobbyistsXml&fileType="
    PERSON_FILE_TYPE = "ACCREDITED_PERSONS"
    ORGANIZATION_FILE_TYPE = "NEW"

    dataset_mapping = {
        "person": {
            "url": DOWNLOAD_URL + PERSON_FILE_TYPE,
            "schema": Person,
            "file_name": "persons_raw.xml"
        },
        "organization": {
            "url": DOWNLOAD_URL + ORGANIZATION_FILE_TYPE,
            "schema": Organization,
            "file_name": "organizations_raw.xml"
        }
    }

    ORGANIZATION_URL = DOWNLOAD_URL + ORGANIZATION_FILE_TYPE

    def __init__(self, file_path):
        pass
        # todo : self.producer = Producer()

    @staticmethod
    def download_data_set(url, filename):
        # Translate url into a filename
        if not os.path.exists(Path('raw_data') / filename):
            log.info(f"Starting download of {url} to {Path('raw_data') / filename}")
            header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '}
            req = Request(url=url, headers=header)

            # Create an HTTP response object
            with urlopen(req) as response:
                # Create a file object
                with open(filename, "wb") as f:
                    # Copy the binary content of the response to the file
                    copyfileobj(response, f)

            log.info(f"Finished download for {filename}")
        else:
            log.info(f"File {filename} already exists.. skipping download")

    def extract(self):
        for v in TransparencyRegisterExtractor.dataset_mapping.values():
            self.download_data_set(v["url"], v["filename"])




