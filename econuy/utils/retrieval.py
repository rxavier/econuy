import ssl
from io import BytesIO
from typing import Literal
from pathlib import Path

import httpx

from econuy.utils.operations import get_project_root


def get_certs_path(source: Literal["bcu", "ine", "inac", "bcra"]) -> Path:
    return Path(get_project_root(), "utils", "files", f"{source}_certs.pem")


def get_with_ssl_context(
    source: Literal["bcu", "ine", "inac", "bcra"], url: str
) -> bytes:
    certs_path = get_certs_path(source)
    ssl_context = ssl.create_default_context(cafile=str(certs_path))

    r = httpx.get(url, verify=ssl_context)
    return BytesIO(r.content)
