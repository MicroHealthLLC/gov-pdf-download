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
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
        'Referer': 'https://www.dhs.gov/'
    }


def get_category_links(driver, base_url):
    """
    Extract all category links from the left sidebar.
    """
    print(f"\nLoading main page: {base_url}")
    driver.get(base_url)
    
    # Simulate human behavior
    time.sleep(random.uniform(3, 5))
    driver.execute_script("window.scrollTo(0, 300);")
    time.sleep(random.uniform(1, 2))
    
    # Wait for sidebar to load
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "usa-sidenav"))
        )
    except TimeoutException:
        print("⚠ Sidebar not found, trying alternative selector...")
    
    # Get page source and parse
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Find sidebar navigation - adjust selector based on actual HTML structure
    category_links = []
    
    # Try multiple selector strategies
    sidebar = soup.find('nav', class_='usa-sidenav') or soup.find('ul', class_='usa-sidenav__sublist')
    
    if sidebar:
        links = sidebar.find_all('a', href=True)
        for link in links:
            href = link.get('href')
            text = link.get_text(strip=True)
            
            # Filter out empty or non-category links
            if href and text and '/publications-library/' in href:
                full_url = urljoin(base_url, href)
                category_links.append({
                    'name': text,
                    'url': full_url
                })
                print(f"  ✓ Found category: {text}")
    
    return category_links


def get_publication_links_from_category(driver, category_url, category_name):
    """
    Get all publication detail page links from a category page.
    """
    print(f"\n{'='*60}")
    print(f"Processing category: {category_name}")
    print(f"{'='*60}")
    
    driver.get(category_url)
    
    # Simulate human behavior
    time.sleep(random.uniform(3, 5))
    driver.execute_script("window.scrollTo(0, 500);")
    time.sleep(random.uniform(1, 2))
    driver.execute_script("window.scrollTo(0, 1000);")
    time.sleep(random.uniform(1, 2))
    
    publication_links = []
    
    # Parse the page
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Find all publication links - these typically have specific patterns
    # Adjust selectors based on actual HTML structure
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link.get('href')
        text = link.get_text(strip=True)
        
        # Look for links that go to publication detail pages
        # Based on your example: /publication/best-practices-safe-conduct-academic-institutions
        if href and '/publication/' in href and text:
            full_url = urljoin(category_url, href)
            
            # Avoid duplicates
            if not any(pub['url'] == full_url for pub in publication_links):
                publication_links.append({
                    'title': text,
                    'url': full_url,
                    'category': category_name
                })
                print(f"  ✓ {text}")
    
    return publication_links


def extract_pdf_from_publication_page(driver, publication_url, publication_title):
    """
    Navigate to publication detail page and extract PDF link.
    """
    print(f"\n  → Opening: {publication_title}")
    driver.get(publication_url)
    
    # Simulate human behavior
    time.sleep(random.uniform(2, 4))
    driver.execute_script("window.scrollTo(0, 400);")
    time.sleep(random.uniform(1, 2))
    
    # Parse the page
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Find PDF link - look in attachment table or direct links
    pdf_url = None
    pdf_filename = None
    
    # Strategy 1: Look for links in attachment table
    attachment_links = soup.find_all('a', href=True)
    for link in attachment_links:
        href = link.get('href')
        if href and '.pdf' in href.lower():
            pdf_url = urljoin(publication_url, href)
            # Try to get filename from link text or href
            link_text = link.get_text(strip=True)
            if link_text and link_text.lower().endswith('.pdf'):
                pdf_filename = link_text
            else:
                pdf_filename = os.path.basename(urlparse(pdf_url).path)
            break
    
    # Strategy 2: Look for download buttons or specific classes
    if not pdf_url:
        download_links = soup.find_all(['a', 'button'], class_=lambda x: x and 'download' in x.lower())
        for link in download_links:
            href = link.get('href')
            if href and '.pdf' in href.lower():
                pdf_url = urljoin(publication_url, href)
                pdf_filename = os.path.basename(urlparse(pdf_url).path)
                break
    
    if pdf_url:
        print(f"    ✓ Found PDF: {pdf_filename}")
        return {
            'url': pdf_url,
            'filename': pdf_filename,
            'title': publication_title,
            'source_page': publication_url
        }
    else:
        print(f"    ✗ No PDF found")
        return None


def download_pdf(pdf_info, output_dir, session):
    """
    Download a single PDF file.
    """
    try:
        filepath = os.path.join(output_dir, pdf_info['filename'])
        
        if os.path.exists(filepath):
            print(f"  ⊙ Already exists: {pdf_info['filename']}")
            return True
        
        print(f"  ↓ Downloading: {pdf_info['filename']}")
        
        # Random delay to mimic human
        time.sleep(random.uniform(2, 4))
        
        response = session.get(pdf_info['url'], timeout=60)
        response.raise_for_status()
        
        # Verify it's actually a PDF
        if not response.content.startswith(b'%PDF'):
            print(f"    ⚠ Not a valid PDF")
            return False
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"    ✓ Downloaded: {len(response.content) / 1024:.2f} KB")
        return True
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)}")
        return False


def scrape_dhs_publications(base_url, output_dir):
    """
    Main scraping function for DHS publications.
    """
    driver = setup_realistic_browser()
    all_pdfs = []
    
    try:
        # Step 1: Get all category links
        category_links = get_category_links(driver, base_url)
        
        if not category_links:
            print("\n✗ No category links found. Check selectors.")
            return []
        
        print(f"\n✓ Found {len(category_links)} categories")
        
        # Step 2: Process each category
        for idx, category in enumerate(category_links, 1):
            print(f"\n[{idx}/{len(category_links)}] Processing: {category['name']}")
            
            # Get publication links from category
            pub_links = get_publication_links_from_category(
                driver, category['url'], category['name']
            )
            
            # Step 3: Visit each publication page and extract PDF
            for pub in pub_links:
                pdf_info = extract_pdf_from_publication_page(
                    driver, pub['url'], pub['title']
                )
                
                if pdf_info:
                    pdf_info['category'] = category['name']
                    all_pdfs.append(pdf_info)
                
                # Human-like delay between publications
                time.sleep(random.uniform(2, 4))
            
            # Human-like delay between categories
            if idx < len(category_links):
                delay = random.uniform(5, 10)
                print(f"\n  Waiting {delay:.1f}s before next category...")
                time.sleep(delay)
        
        return all_pdfs
        
    finally:
        driver.quit()


if __name__ == "__main__":
    print("DHS Publications Library Scraper (HEADLESS MODE)")
    print("="*60 + "\n")
    
    base_url = 'https://www.dhs.gov/publications-library/academic-engagement'
    output_dir = 'dhs_pdfs'
    
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Scrape all publications
    print("Starting scrape...")
    all_pdfs = scrape_dhs_publications(base_url, output_dir)
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total PDFs found: {len(all_pdfs)}")
    
    # Organize by category
    categories = {}
    for pdf in all_pdfs:
        cat = pdf.get('category', 'Unknown')
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(pdf)
    
    for cat, pdfs in categories.items():
        print(f"  {cat}: {len(pdfs)} PDFs")
    
    # Download prompt
    if all_pdfs:
        proceed = input("\nDownload all PDFs? (yes/no): ").strip().lower()
        if proceed in ['yes', 'y']:
            session = requests.Session()
            session.headers.update(get_realistic_headers())
            
            print(f"\n{'='*60}")
            print("DOWNLOADING PDFs")
            print(f"{'='*60}\n")
            
            for idx, pdf in enumerate(all_pdfs, 1):
                print(f"[{idx}/{len(all_pdfs)}] {pdf['title']}")
                download_pdf(pdf, output_dir, session)
            
            print(f"\n✓ Download complete! Files saved to: {output_dir}")
    else:
        print("\n✗ No PDFs found")
