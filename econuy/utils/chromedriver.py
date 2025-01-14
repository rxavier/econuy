import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def _build(download_dir: str = "."):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        },
    )

    if os.environ.get("IN_DOCKER") == "true":  # fix for webapp
        service = Service(executable_path="/usr/bin/chromedriver")
        chrome_options.add_argument("--disable-dev-shm-usage")
    else:
        service = None

    return webdriver.Chrome(service=service, options=chrome_options)
