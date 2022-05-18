import logging
import os

import click

from transparency_register_crawler.extractor import TransparencyRegisterExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
def run():
    TransparencyRegisterExtractor().extract()


if __name__ == "__main__":
    run()
