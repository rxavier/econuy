import logging
import random
from string import ascii_letters
from pathlib import Path
from typing import Optional


def setup(file: Optional[str] = None, null: bool = False):
    """Setup logging."""
    name = ''.join(random.choice(ascii_letters) for i in range(10))
    log_obj = logging.getLogger(name)

    if null is True:
        log_obj.addHandler(logging.NullHandler())
        return log_obj

    log_obj.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(message)s',
                                  datefmt="%d-%b %T")
    ch.setFormatter(formatter)
    log_obj.addHandler(ch)

    if file is not None:
        file_ext = Path(file).with_suffix(".log")
        fh = logging.FileHandler(file_ext)
        fh.setFormatter(formatter)
        log_obj.addHandler(fh)

    return log_obj
