from pathlib import Path

from eliot import to_file

LOG_PATH = f"{Path(__file__).parent.resolve().absolute()}/file.log"

__LOGGER__ = to_file(open(LOG_PATH, "w"))
