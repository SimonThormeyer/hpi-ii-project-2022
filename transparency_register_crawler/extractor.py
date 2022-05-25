import logging
import os
import re
from shutil import copyfileobj
from typing import List, Type
from urllib.request import Request, urlopen
from pathlib import Path
from nested_lookup import nested_alter, nested_delete

import xmltodict
from tqdm import tqdm

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
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
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
                                               'interestRepresentative'],
                                           desc='Parsing organizations'):
            TransparencyRegisterExtractor.cast_datatypes(interestRepresentative)
            org = Organization(**interestRepresentative)
            organizations.append(org)
        return organizations

    @staticmethod
    def cast_datatypes(organizations_dict):
        nested_alter(organizations_dict, "membersFTE", TransparencyRegisterExtractor.decimal_string_to_int,
                     in_place=True)
        nested_alter(organizations_dict, "members100Percent",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, int), in_place=True)
        nested_alter(organizations_dict, "members75Percent",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, int), in_place=True)
        nested_alter(organizations_dict, "members50Percent",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, int), in_place=True)
        nested_alter(organizations_dict, "members25Percent",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, int), in_place=True)
        nested_alter(organizations_dict, "members10Percent",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, int), in_place=True)
        nested_alter(organizations_dict, "newOrganisation",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, bool), in_place=True)
        nested_alter(organizations_dict, "absoluteCost",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, float), in_place=True)
        nested_alter(organizations_dict, "members", TransparencyRegisterExtractor.decimal_string_to_int, in_place=True)

        nested_alter(organizations_dict, "clients", TransparencyRegisterExtractor.forward_list_layer, in_place=True)

        nested_alter(organizations_dict, "grants",
                     TransparencyRegisterExtractor.forward_list_layer, in_place=True)

        nested_alter(organizations_dict, "min",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, float), in_place=True)
        nested_alter(organizations_dict, "max",
                     lambda value: TransparencyRegisterExtractor.string_to_type(value, float), in_place=True)

        nested_alter(organizations_dict, "costs",
                     lambda value: TransparencyRegisterExtractor.rename_child_key(value, "@currency", "currency"),
                     in_place=True)
        nested_alter(organizations_dict, "totalAnnualRevenue",
                     lambda value: TransparencyRegisterExtractor.rename_child_key(value, "@currency", "currency"),
                     in_place=True)
        nested_alter(organizations_dict, "fundingSources", TransparencyRegisterExtractor.forward_list_layer,
                     in_place=True)
        nested_alter(organizations_dict, "contributions", TransparencyRegisterExtractor.forward_list_layer,
                     in_place=True)
        nested_alter(organizations_dict, "intermediaries", TransparencyRegisterExtractor.forward_list_layer,
                     in_place=True)
        nested_alter(organizations_dict, "interests",
                     TransparencyRegisterExtractor.forward_list_layer,
                     in_place=True)
        nested_alter(organizations_dict, "levelsOfInterest",
                     TransparencyRegisterExtractor.forward_list_layer,
                     in_place=True)

        nested_delete(organizations_dict, "@xmlns:xsi", in_place=True)
        nested_delete(organizations_dict, "@xsi:type", in_place=True)

        # string lists
        nested_alter(organizations_dict, "EULegislativeProposals",
                     TransparencyRegisterExtractor.split_string_by_semicolon,
                     in_place=True)
        nested_alter(organizations_dict, "communicationActivities",
                     TransparencyRegisterExtractor.split_string_by_newline,
                     in_place=True)
        nested_alter(organizations_dict, "organisationMembers",
                     TransparencyRegisterExtractor.split_string_by_newline,
                     in_place=True)

    @staticmethod
    def rename_child_key(value: dict, old_key: str, new_key: str):
        if type(value) == dict and old_key in value:
            assert new_key not in value
            value[new_key] = value[old_key]
            value.pop(old_key)
            return value
        return value

    @staticmethod
    def forward_list_layer(value: dict):
        if type(value) == dict:
            result = list(value.values())[0]
            result = result if type(result) == list else [result]
            return result
        return value

    @staticmethod
    def forward_dict_layer(value: dict):
        if type(value) == dict:
            values = list(value.values())
            if type(values[0]) == dict:
                return list(value.values())[0]
        return value

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

    @staticmethod
    def split_string_by_newline(value: str):
        return value.split(sep="\r\n")

    @staticmethod
    def split_string_by_semicolon(value: str):
        return re.split(r";\s+", value)
