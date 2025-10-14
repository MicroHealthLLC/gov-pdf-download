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
    Runs in HEADLESS MODE.
    """
    chrome_options = Options()
    
    # HEADLESS MODE
    chrome_options.add_argument('--headless=new')
    
    # Anti-detection measures
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic window size
    chrome_options.add_argument('--window-size=1920,1080')
    
    # Realistic user agent
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Additional settings
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
    Return comprehensive, realistic browser headers for Coast Guard sites.
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
        'Referer': 'https://www.dcms.uscg.mil/'
    }

def setup_realistic_browser():
    """
    Configure Chrome with realistic browser fingerprint to avoid detection.
    Runs in HEADLESS MODE with enhanced headers.
    """
    chrome_options = Options()
    
    # HEADLESS MODE
    chrome_options.add_argument('--headless=new')
    
    # Anti-detection measures
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic window size
    chrome_options.add_argument('--window-size=1920,1080')
    
    # Realistic user agent - CRITICAL for end-user identification
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    
    # Additional settings
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    
    # Language and locale
    chrome_options.add_argument('--lang=en-US')
    chrome_options.add_experimental_option('prefs', {
        'intl.accept_languages': 'en-US,en;q=0.9',
        'profile.default_content_setting_values.notifications': 2,
        'profile.managed_default_content_settings.images': 1
    })
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # Enhanced CDP commands for realistic browser fingerprint
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        "platform": "Windows",
        "acceptLanguage": "en-US,en;q=0.9"
    })
    
    # Remove webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Add realistic browser properties that identify as end-user
    driver.execute_script("""
        // Override navigator properties
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                {name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                {name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                {name: 'Native Client', description: '', filename: 'internal-nacl-plugin'}
            ]
        });
        
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Win32'
        });
        
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8
        });
        
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8
        });
        
        Object.defineProperty(navigator, 'maxTouchPoints', {
            get: () => 0
        });
        
        // Add Chrome runtime
        window.chrome = {
            runtime: {}
        };
        
        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({state: Notification.permission}) :
                originalQuery(parameters)
        );
    """)
    
    return driver


def get_realistic_headers(referer_url='https://www.dcms.uscg.mil/'):
    """
    Return comprehensive, realistic browser headers that identify as end-user.
    These headers are critical for avoiding bot detection.
    """
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',  # Changed from 'none' for better realism
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'Referer': referer_url,  # Dynamic referer based on source
        'Sec-CH-UA-Full-Version-List': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"'
    }


def extract_coast_guard_pdfs(url, output_dir):
    """
    Extract PDFs from Coast Guard DCMS pages with enhanced anti-detection.
    """
    
    driver = setup_realistic_browser()
    all_pdfs = []
    
    try:
        print(f"\nLoading: {url}")
        
        # Set additional headers via CDP (Chrome DevTools Protocol)
        driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
            'headers': get_realistic_headers(url)
        })
        
        # Enable network tracking
        driver.execute_cdp_cmd('Network.enable', {})
        
        driver.get(url)
        
        # Mimic human behavior - MORE REALISTIC TIMING
        print("Simulating human behavior...")
        time.sleep(random.uniform(3, 6))
        
        # Random scrolling with realistic pauses
        scroll_positions = [300, 600, 1000, 1500, 2000]
        for pos in scroll_positions:
            driver.execute_script(f"window.scrollTo(0, {pos});")
            time.sleep(random.uniform(1.5, 3.5))
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(random.uniform(2, 4))
        
        # Wait for content to load
        print("Waiting for page content...")
        time.sleep(random.uniform(8, 12))
        
        # Get HTML
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Strategy 1: Find table rows and extract PDF links
        tables = soup.find_all('table')
        print(f"Found {len(tables)} table(s)")
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    links = cell.find_all('a', href=True)
                    for link in links:
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
        
        # Strategy 2: General PDF link search (fallback)
        if len(all_pdfs) == 0:
            print("Table search found no PDFs, trying general link search...")
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
        
        print(f"\nTotal PDFs found: {len(all_pdfs)}")
        return all_pdfs
        
    except Exception as e:
        print(f"Error extracting PDFs: {str(e)}")
        return []
        
    finally:
        driver.quit()
def extract_coast_guard_pdfs(url, output_dir):
    """
    Extract PDFs from Coast Guard DCMS pages with table structure.
    Targets the 'Title' column links that point directly to PDFs.
    """
    
    driver = setup_realistic_browser()
    all_pdfs = []
    
    try:
        print(f"\nLoading: {url}")
        driver.get(url)
        
        # Mimic human behavior
        print("Simulating human behavior...")
        time.sleep(5)
        
        # Random scrolling
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 1500);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(3)
        
        # Wait for content to load
        print("Waiting for page content...")
        time.sleep(10)
        
        # Get HTML
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Strategy 1: Find table rows and extract PDF links from 'Title' column
        tables = soup.find_all('table')
        print(f"Found {len(tables)} table(s)")
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                # Look for links in table cells (Title column typically has the PDF link)
                for cell in cells:
                    links = cell.find_all('a', href=True)
                    for link in links:
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
        
        # Strategy 2: General PDF link search (fallback)
        if len(all_pdfs) == 0:
            print("Table search found no PDFs, trying general link search...")
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
        
        print(f"\nTotal PDFs found: {len(all_pdfs)}")
        return all_pdfs
        
    except Exception as e:
        print(f"Error extracting PDFs: {str(e)}")
        return []
        
    finally:
        driver.quit()


def download_pdfs(pdf_list, output_dir):
    """Download PDFs with realistic headers and error handling."""
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    session = requests.Session()
    session.headers.update(get_realistic_headers())
    
    for idx, pdf in enumerate(pdf_list, 1):
        try:
            # Clean filename
            filename = os.path.basename(pdf['url'].split('?')[0])
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            
            filepath = os.path.join(output_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(pdf_list)}] ⊙ {filename} (already exists)")
                continue
            
            print(f"[{idx}/{len(pdf_list)}] ↓ {filename}")
            
            # Add random delay to mimic human
            time.sleep(random.uniform(2, 5))
            
            response = session.get(pdf['url'], timeout=60, allow_redirects=True)
            response.raise_for_status()
            
            # Verify it's actually a PDF
            if not response.content.startswith(b'%PDF'):
                print(f"  ⚠ Not a valid PDF file")
                continue
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"  ✓ Downloaded {len(response.content) / 1024:.2f} KB")
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Download error: {str(e)}")
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")


def scrape_multiple_urls(urls, base_output_dir):
    """
    Scrape multiple Coast Guard URLs and organize PDFs by source.
    """
    all_pdfs_by_source = {}
    
    for idx, url in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"Processing URL {idx}/{len(urls)}")
        print(f"{'='*60}")
        
        # Extract source name from URL for organizing
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip('/').split('/')
        source_name = path_parts[-1] if path_parts[-1] else path_parts[-2]
        source_name = source_name.replace('.aspx', '').replace('-', '_')
        
        output_dir = os.path.join(base_output_dir, source_name)
        
        try:
            pdfs = extract_coast_guard_pdfs(url, output_dir)
            all_pdfs_by_source[source_name] = {
                'url': url,
                'pdfs': pdfs,
                'output_dir': output_dir
            }
            
            print(f"\n✓ Found {len(pdfs)} PDFs from {source_name}")
            
            # Human-like delay between URLs
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
    print("Coast Guard DCMS - Multi-URL PDF Scraper (HEADLESS MODE)")
    print("="*60 + "\n")
    
    # Coast Guard URLs to scrape
    urls_to_scrape = [
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Instructions/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Instruction-Manuals/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/DCMS-Instructions/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Interagency-Agreements/'
    ]
    
    base_output_dir = 'coast_guard_pdfs'
    
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
