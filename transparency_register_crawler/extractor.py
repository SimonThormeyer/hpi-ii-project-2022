import logging
import os
from shutil import copyfileobj
from typing import List
from urllib.request import Request, urlopen
from pathlib import Path

from build.gen.bakdata.person.v1.person_pb2 import Person  # type: ignore
from build.gen.bakdata.organization.v1.organization_pb2 import Organization  # type: ignore
from transparency_register_crawler.organization_producer import TransRegOrganizationProducer
from transparency_register_crawler.person_producer import TransRegPersonProducer

log = logging.getLogger(__name__)


class TransparencyRegisterExtractor:
    DOWNLOAD_URL = "http://ec.europa.eu/transparencyregister/public/consultation/statistics.do?action=getLobbyistsXml&fileType="
    PERSON_FILE_TYPE = "ACCREDITED_PERSONS"
    ORGANIZATION_FILE_TYPE = "NEW"
    RAW_DATA_PATH = "raw_data"

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

    def __init__(self):
        self.organization_producer = TransRegOrganizationProducer()
        self.person_producer = TransRegPersonProducer()

    @staticmethod
    def download_data_set(url, filename):
        # Translate url into a filename
        file_path = Path(TransparencyRegisterExtractor.RAW_DATA_PATH) / filename
        if not os.path.exists(file_path):
            log.info(f"Starting download of {url} to {file_path}")
            header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '}
            req = Request(url=url, headers=header)

            # Create an HTTP response object
            with urlopen(req) as response:
                # Create a file object
                with open(file_path, "wb") as f:
                    # Copy the binary content of the response to the file
                    copyfileobj(response, f)

            log.info(f"Finished download for {filename}")
        else:
            log.info(f"File {filename} already exists.. skipping download")

    def extract(self):
        for v in TransparencyRegisterExtractor.dataset_mapping.values():
            self.download_data_set(v["url"], v["file_name"])

        # Todo: parse downloaded data into objects for Organization and Person
        persons: List[Person] = []
        for person in persons:
            self.person_producer.produce_to_topic(person)

        organizations: List[Organization] = []
        for organization in organizations:
            self.person_producer.produce_to_topic(organization)




