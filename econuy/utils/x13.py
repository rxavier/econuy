import platform
import os
from pathlib import Path

import httpx


def _get_binary() -> None:
    if "X13PATH" in os.environ:
        return os.environ["X13PATH"]

    urls = {
        "Windows": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/x13/windows/x13as.exe",
        "Darwin-x64": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/x13/darwin/x64/x13as",
        "Darwin-arm64": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/x13/darwin/arm64/x13as",
        "Linux-x64": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/x13/linux/x64/x13as",
        "Linux-arm64": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/x13/linux/arm64/x13as",
    }

    system_string = platform.system()
    if system_string == "Windows":
        suffix = ".exe"
        base_dir = Path.home() / "AppData" / "Roaming" / "econuy"
    else:
        suffix = ""
        base_dir = Path.home() / ".econuy"
    if system_string == "Darwin":
        if "ARM64" in platform.version():
            system_string += "-arm64"
        else:
            system_string += "-x64"
        base_dir = Path.home() / "Library" / "Application Support" / "econuy"
    elif system_string == "Linux":
        if "aarch64" in platform.machine():
            system_string += "-arm64"
        else:
            system_string += "-x64"
    if system_string not in urls.keys():
        raise ValueError(
            "X13 binaries are only available for Windows, Darwin (macOS) or Linux."
        )

    base_dir.mkdir(exist_ok=True, parents=True)
    binary_path = Path(base_dir, f"x13as{suffix}")

    if binary_path.exists():
        os.chmod(binary_path, 0o755)
        os.environ["X13PATH"] = binary_path.as_posix()
        print(f"Using existing binary at {binary_path}")
        return binary_path.as_posix()

    r = httpx.get(urls[system_string])
    r.raise_for_status()
    with open(binary_path, "wb") as f:
        f.write(r.content)
    os.chmod(binary_path, 0o755)
    os.environ["X13PATH"] = binary_path.as_posix()

    print(
        f"Download complete. Saved binary to {base_dir} and set X13PATH to {binary_path}"
    )

    return binary_path.as_posix()
