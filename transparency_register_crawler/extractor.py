import logging
import os
from shutil import copyfileobj
from typing import List, Dict, Any
from urllib.request import Request, urlopen
from pathlib import Path
from nested_lookup import nested_alter, nested_delete

import xmltodict

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

        persons: List[Person] = self.parse_persons()
        organizations: List[Organization] = self.parse_organizations()

        for person in persons:
            self.person_producer.produce_to_topic(person)

        for organization in organizations:
            self.organization_producer.produce_to_topic(organization)

    @staticmethod
    def xml_to_dict(key: str):
        file_name = TransparencyRegisterExtractor.dataset_mapping[key]["file_name"]
        xml = open(Path(TransparencyRegisterExtractor.RAW_DATA_PATH) / file_name, "r", encoding="utf-8")
        return xmltodict.parse(xml.read())

    def parse_persons(self) -> List[Person]:
        persons_dict = self.xml_to_dict("person")
        return [Person(**accreditedPerson) for accreditedPerson in
                persons_dict["ListOfAccreditedPerson"]["resultList"]["accreditedPerson"]]

    def parse_organizations(self) -> List[Organization]:
        organizations_dict = self.xml_to_dict("organization")
        # organizations_dict = TransparencyRegisterExtractor.cast_datatypes_for_real(organizations_dict)
        organizations = []
        for interestRepresentative in tqdm(organizations_dict['ListOfIRPublicDetail']["resultList"][
            'interestRepresentative'], total=len(organizations_dict['ListOfIRPublicDetail']["resultList"][
            'interestRepresentative'])):
            TransparencyRegisterExtractor.cast_datatypes_for_real(interestRepresentative)
            org = Organization(**interestRepresentative)
            organizations.append(org)
        return organizations

    @staticmethod
    def cast_datatypes(interest_representative: Dict[str, Any]):
        interest_representative['name'] = interest_representative['name']["originalName"]
        if 'members100Percent' in interest_representative['members']:
            interest_representative['members']['members100Percent'] = int(
                interest_representative['members']['members100Percent']
            )
        if 'members50Percent' in interest_representative['members']:
            interest_representative['members']['members50Percent'] = int(
                interest_representative['members']['members50Percent']
            )
        interest_representative['members']['members'] = int(float(
            interest_representative['members']['members']))

        interest_representative['members']['membersFTE'] = int(float(
            interest_representative['members']['membersFTE']))

        interest_representative["financialData"]["newOrganisation"] = bool(
            interest_representative["financialData"]["newOrganisation"])

        closed_year = interest_representative["financialData"]["closedYear"]

        if 'costs' in closed_year:
            closed_year['costs']['range']['min'] = float(
                closed_year['costs']['range']['min'])

            closed_year['costs']['range']['max'] = float(
                closed_year['costs']['range']['max'])
        if 'grants' in closed_year:
            closed_year['grants']["grant"]["amount"]["absoluteCost"] = float(
                closed_year['grants']["grant"]["amount"]["absoluteCost"])
            closed_year['grants'] = closed_year['grants']["grant"]

    @staticmethod
    def decimal_string_to_int(value: str):
        if type(value) == str:
            return int(float(value))
        return value

    @staticmethod
    def string_to_type(value: str, t: Type):
        if type(value) == str:
            return t(value)
        return value
