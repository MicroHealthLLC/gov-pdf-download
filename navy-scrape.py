import os
import time
import random
from urllib.parse import urljoin, urlparse
import requests

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
except ImportError:
    print("Install: pip install selenium beautifulsoup4 requests")
    exit(1)


def setup_realistic_browser():
    """
    Configure Chrome with realistic browser fingerprint to avoid detection.
    NOW RUNS IN HEADLESS MODE - won't render UI on your screen.
    """
    chrome_options = Options()
    
    # HEADLESS MODE - This prevents the browser from rendering on screen
    chrome_options.add_argument('--headless=new')
    
    # Anti-detection measures
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic window size (still needed for headless rendering)
    chrome_options.add_argument('--window-size=1920,1080')
    
    # Realistic user agent
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Additional anti-detection and performance settings
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    
    # Language and locale
    chrome_options.add_argument('--lang=en-US')
    chrome_options.add_experimental_option('prefs', {
        'intl.accept_languages': 'en-US,en;q=0.9'
    })
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # Remove webdriver property
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Add realistic browser properties
    driver.execute_script("""
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
    """)
    
    return driver


def get_realistic_headers():
    """
    Return comprehensive, realistic browser headers.
    """
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://www.secnav.navy.mil/'
    }


def extract_navy_pdfs(url, output_dir):
    """
    Extract PDFs with enhanced anti-detection.
    """
    
    driver = setup_realistic_browser()
    all_pdfs = []
    
    try:
        print(f"\nLoading: {url}")
        driver.get(url)
        
        # Mimic human behavior
        print("Simulating human behavior...")
        time.sleep(5)
        
        # Random mouse movements and scrolling
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(3)
        
        # Wait for content
        print("Waiting for SharePoint content...")
        time.sleep(15)
        
        # Get HTML
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Find all PDF links
        all_links = soup.find_all('a', href=True)
        print(f"Found {len(all_links)} total links")
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if '.pdf' in href.lower():
                full_url = urljoin(url, href)
                
                if not any(pdf['url'] == full_url for pdf in all_pdfs):
                    all_pdfs.append({
                        'url': full_url,
                        'name': text if text else os.path.basename(urlparse(full_url).path)
                    })
                    print(f"  ✓ {text}")
        
        return all_pdfs
        
    finally:
        driver.quit()


def download_pdfs(pdf_list, output_dir):
    """Download PDFs with realistic headers."""
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    session = requests.Session()
    session.headers.update(get_realistic_headers())
    
    for idx, pdf in enumerate(pdf_list, 1):
        try:
            filename = os.path.basename(pdf['url'].split('?')[0])
            filepath = os.path.join(output_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(pdf_list)}] ⊙ {filename}")
                continue
            
            print(f"[{idx}/{len(pdf_list)}] ↓ {filename}")
            
            # Add random delay to mimic human
            time.sleep(random.uniform(2, 5))
            
            response = session.get(pdf['url'], timeout=60)
            response.raise_for_status()
            
            if not response.content.startswith(b'%PDF'):
                print(f"  ⚠ Not PDF")
                continue
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"  ✓ {len(response.content) / 1024:.2f} KB")
            
        except Exception as e:
            print(f"  ✗ {str(e)}")


def scrape_multiple_urls(urls, base_output_dir):
    """
    Scrape multiple URLs and organize PDFs by source.
    """
    all_pdfs_by_source = {}
    
    for idx, url in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"Processing URL {idx}/{len(urls)}")
        print(f"{'='*60}")
        
        # Extract source name from URL for organizing
        source_name = urlparse(url).path.strip('/').split('/')[-1].replace('.aspx', '')
        output_dir = os.path.join(base_output_dir, source_name)
        
        try:
            pdfs = extract_navy_pdfs(url, output_dir)
            all_pdfs_by_source[source_name] = {
                'url': url,
                'pdfs': pdfs,
                'output_dir': output_dir
            }
            
            print(f"\n✓ Found {len(pdfs)} PDFs from {source_name}")
            
            # Human-like delay between URLs (random 10-20 seconds)
            if idx < len(urls):
                delay = random.uniform(10, 20)
                print(f"\nWaiting {delay:.1f}s before next URL...")
                time.sleep(delay)
                
        except Exception as e:
            print(f"\n✗ Error processing {url}: {str(e)}")
            all_pdfs_by_source[source_name] = {
                'url': url,
                'pdfs': [],
                'output_dir': output_dir,
                'error': str(e)
            }
    
    return all_pdfs_by_source


if __name__ == "__main__":
    print("Navy SECNAV - Multi-URL Scraper (HEADLESS MODE)")
    print("="*60 + "\n")
    
    # All URLs to scrape
    urls_to_scrape = [
        'https://www.secnav.navy.mil/doni/notices.aspx',
        'https://www.secnav.navy.mil/doni/allinstructions.aspx',
        'https://www.secnav.navy.mil/doni/manuals-secnav.aspx',
        'https://www.secnav.navy.mil/doni/manuals-opnav.aspx',
        'https://www.secnav.navy.mil/doni/navalintelligence.aspx'
    ]
    
    base_output_dir = 'navy_pdfs'
    
    # Scrape all URLs
    results = scrape_multiple_urls(urls_to_scrape, base_output_dir)
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}\n")
    
    total_pdfs = 0
    for source_name, data in results.items():
        pdf_count = len(data['pdfs'])
        total_pdfs += pdf_count
        status = "✓" if pdf_count > 0 else "✗"
        print(f"{status} {source_name}: {pdf_count} PDFs")
        if 'error' in data:
            print(f"  Error: {data['error']}")
    
    print(f"\nTotal PDFs found: {total_pdfs}")
    
    # Download prompt
    if total_pdfs > 0:
        proceed = input("\nDownload all PDFs? (yes/no): ").strip().lower()
        if proceed in ['yes', 'y']:
            for source_name, data in results.items():
                if data['pdfs']:
                    print(f"\n{'='*60}")
                    print(f"Downloading from: {source_name}")
                    print(f"{'='*60}")
                    download_pdfs(data['pdfs'], data['output_dir'])
    else:
        print("\n✗ No PDFs found across all sources")