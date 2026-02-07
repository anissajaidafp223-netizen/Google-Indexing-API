import requests
import xml.etree.ElementTree as ET
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
import time
import tempfile
import gzip
import io
import random
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

SCOPES = ['https://www.googleapis.com/auth/indexing']
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SITEMAP_URL = os.getenv("SITEMAP_URL")
INDEXING_API_ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

def fetch_sitemap_urls(sitemap_url):
    try:
        logging.info(f"Fetching sitemap: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=15)
        response.raise_for_status()
        content = response.content
        if sitemap_url.endswith('.gz') or 'gzip' in response.headers.get('Content-Type', ''):
            logging.info("Gzip detected, decompressing...")
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = ET.fromstring(content)
       locs = root.findall(".//{*}loc")
urls = [url.text for url in locs if url.text]
        logging.info(f"Found {len(urls)} URLs")
        return urls
    except Exception as e:
        logging.error(f"Sitemap error: {e}")
        return []

def notify_google(session, credentials, url):
    body = {"url": url, "type": "URL_UPDATED"}
    headers = {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}
    try:
        response = session.post(INDEXING_API_ENDPOINT, json=body, headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(f"Success: {url}")
        else:
            logging.error(f"Failed ({response.status_code}): {url}")
    except Exception as e:
        logging.error(f"Error: {url} - {e}")

def main():
    try:
        if not GOOGLE_CREDENTIALS_JSON or not SITEMAP_URL:
            logging.error("Missing environment variables")
            return

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_file:
            temp_file.write(GOOGLE_CREDENTIALS_JSON)
            temp_file.flush()
            credentials = service_account.Credentials.from_service_account_file(temp_file.name, scopes=SCOPES)
        
        if not credentials.valid:
            credentials.refresh(Request())
            
        urls = fetch_sitemap_urls(SITEMAP_URL)
        if not urls:
            logging.error("No URLs found.")
            return

        # Randomize to cover all products over time
        logging.info("Shuffling URLs for full coverage...")
        random.shuffle(urls) 

        session = requests.Session()
        with ThreadPoolExecutor(max_workers=5) as executor:
            # We push all; Google handles the 200 quota via 429 errors
            for url in urls:
                executor.submit(notify_google, session, credentials, url)
        
        logging.info("Task finished.")
    except Exception as e:
        logging.error(f"Runtime error: {e}")

if __name__ == "__main__":
    main()
