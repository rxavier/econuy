from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def _build(download_dir: str = '.'):
    """Utility function for building a Selenium Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option('prefs', {
    'download.default_directory': download_dir,
    'download.prompt_for_download': False,
    'download.directory_upgrade': True
})
    return webdriver.Chrome(options=chrome_options)
