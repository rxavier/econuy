import os
from pathlib import Path

import chromedriver_autoinstaller
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options

from econuy.utils import get_project_root


def _build():
    """Utility function for building a Selenium Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    try:
        load_dotenv(Path(get_project_root(), ".env"))
        chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_PATH")
        return webdriver.Chrome(
            executable_path=os.environ.get("CHROMEDRIVER_PATH"),
            options=chrome_options)
    except (WebDriverException, TypeError):
        chromedriver_autoinstaller.install()
        return webdriver.Chrome(options=chrome_options)
