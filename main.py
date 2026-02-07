import gzip
import io

def fetch_sitemap_urls(sitemap_url):
    try:
        response = requests.get(sitemap_url, timeout=10)
        response.raise_for_status()
        
        # 自动识别并解压 .gz 文件
        content = response.content
        if sitemap_url.endswith('.gz') or response.headers.get('Content-Type') in ['application/x-gzip', 'application/gzip']:
            logging.info("正在解压 .gz 格式的站点地图...")
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        
        root = ET.fromstring(content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        return [url.text for url in root.findall(".//ns:loc", namespace)]
    except Exception as e:
        logging.error(f"解析站点地图失败: {e}")
        return []
