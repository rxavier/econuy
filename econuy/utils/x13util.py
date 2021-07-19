import platform
import os
from pathlib import Path
from typing import Union

import requests

from econuy.utils import get_project_root


def _get_binary(file_path: Union[str, os.PathLike, None] = None) -> None:
    if file_path is None:
        file_path = Path(get_project_root(), "utils")

    urls = {
        "Windows": "https://github.com/rxavier/econuy-extras/raw/main/econuy_extras/x13/windows/x13as.exe",
        "Darwin": "https://github.com/rxavier/econuy-extras/raw/main/econuy_extras/x13/darwin/x13as",
        "Linux": "https://github.com/rxavier/econuy-extras/raw/main/econuy_extras/x13/linux/x13as",
    }

    system_string = platform.system()
    if system_string not in urls.keys():
        raise ValueError("X13 binaries are only available for Windows, Darwin (macOS) or Linux.")
    if system_string == "Windows":
        suffix = ".exe"
    else:
        suffix = ""

    r = requests.get(urls[system_string])
    binary_path = Path(file_path, f"x13as{suffix}")
    with open(binary_path, "wb") as f:
        f.write(r.content)
    print("Download complete.")
    return binary_path


def _search_binary(
    start_path: Union[str, os.PathLike],
    n: int = 0,
    download_path: Union[str, os.PathLike, None] = None,
):
    """Recursively search for a the X13 binary starting from the n-parent folder of
    a supplied path."""
    if platform.system() == "Windows":
        search_term = "x13as.exe"
    else:
        search_term = "x13as"
    i = 0
    while i < n:
        i += 1
        start_path = os.path.dirname(start_path)
    try:
        final_path = [x for x in Path(start_path).rglob(search_term)][0].absolute().as_posix()
    except IndexError:
        print("X13 binary not found. Downloading appropiate binary for your system...")
        final_path = _get_binary(file_path=download_path).absolute().as_posix()

    return final_path
