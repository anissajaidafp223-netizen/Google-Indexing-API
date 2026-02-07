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
from dotenv import load_dotenv

load_dotenv()

# 1. 日志配置
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

SCOPES = ['https://www.googleapis.com/auth/indexing']
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SITEMAP_URL = os.getenv("SITEMAP_URL")
INDEXING_API_ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

# 2. 抓取并解压地图函数
def fetch_sitemap_urls(sitemap_url):
    try:
        logging.info(f"正在抓取地图: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=15)
        response.raise_for_status()
        content = response.content
        
        # 自动识别并解压 .gz 文件
        if sitemap_url.endswith('.gz') or 'gzip' in response.headers.get('Content-Type', ''):
            logging.info("检测到压缩格式，正在解压...")
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        
        root = ET.fromstring(content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = [url.text for url in root.findall(".//ns:loc", namespace)]
        logging.info(f"成功找到 {len(urls)} 个链接")
        return urls
    except Exception as e:
        logging.error(f"解析地图失败: {e}")
        return []

# 3. 推送给 Google 的函数
def notify_google(session, credentials, url):
    body = {"url": url, "type": "URL_UPDATED"}
    headers = {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}
    try:
        response = session.post(INDEXING_API_ENDPOINT, json=body, headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(f"推送成功: {url}")
        else:
            logging.error(f"推送失败 ({response.status_code}): {url}")
    except Exception as e:
        logging.error(f"推送出错: {url} - {e}")

# 4. 主程序入口
def main():
    try:
        if not GOOGLE_CREDENTIALS_JSON or not SITEMAP_URL:
            logging.error("缺少环境变量！请检查 GitHub Secrets 和 index.yml")
            return

        # 认证
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_file:
            temp_file.write(GOOGLE_CREDENTIALS_JSON)
            temp_file.flush()
            credentials = service_account.Credentials.from_service_account_file(temp_file.name, scopes=SCOPES)
        
        if not credentials.valid:
            credentials.refresh(Request())
            
        # 获取链接并推送
        urls = fetch_sitemap_urls(SITEMAP_URL)
        if not urls:
            logging.error("地图内未找到任何链接，请确认地图链接是否有效。")
            return

        session = requests.Session()
        with ThreadPoolExecutor(max_workers=5) as executor:
            for url in urls:
                executor.submit(notify_google, session, credentials, url)
        
        logging.info("所有任务处理完毕！")
    except Exception as e:
        logging.error(f"运行发生崩溃: {e}")

if __name__ == "__main__":
    main()
