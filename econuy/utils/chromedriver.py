from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def _build():
    """Utility function for building a Selenium Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    return webdriver.Chrome(
            options=chrome_options
        )
