import os
import time
import random
import datetime
from urllib.parse import urljoin, urlparse
import requests
import re
import shutil

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


def setup_realistic_browser(download_dir=None):
    """
    Configure Chrome with realistic browser fingerprint to avoid detection.
    Runs in HEADLESS MODE - No visible browser windows!
    """
    chrome_options = Options()
    
    # HEADLESS MODE
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    
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
    chrome_options.add_argument('--lang=en-US')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--disable-notifications')
    
    # Configure download behavior if download_dir provided
    if download_dir:
        prefs = {
            'download.default_directory': os.path.abspath(download_dir),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'plugins.always_open_pdf_externally': True,
            'plugins.plugins_disabled': ['Chrome PDF Viewer']
        }
        chrome_options.add_experimental_option('prefs', prefs)
    else:
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


def extract_issuance_number(text, href):
    """
    Extract issuance number from link text or href.
    Examples: DoDI 5000.02, DoDD 5100.01, DTM 19-004
    """
    # Common patterns for DoD issuances
    patterns = [
        r'(DoDI?\s*\d+\.\d+)',  # DoDI 5000.02
        r'(DoDD\s*\d+\.\d+)',    # DoDD 5100.01
        r'(DoDM\s*\d+\.\d+)',    # DoDM 5200.01
        r'(DTM\s*\d+-\d+)',      # DTM 19-004
        r'(AI\s*\d+)',           # AI 10
        r'(\d+\.\d+[A-Z]?)',     # 5000.02 or 5000.02E
    ]
    
    # Try to find pattern in text first
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).replace(' ', '_')
    
    # Try href
    for pattern in patterns:
        match = re.search(pattern, href, re.IGNORECASE)
        if match:
            return match.group(1).replace(' ', '_')
    
    return None


def extract_dod_pdfs(url, category_name):
    """
    Extract PDFs from DoD category page.
    Returns list of PDFs with their source page URL for proper referer headers.
    """
    driver = setup_realistic_browser()
    all_pdfs = []
    
    try:
        print(f"\n  Loading category: {category_name}")
        print(f"  URL: {url}")
        driver.get(url)
        
        # Mimic human behavior
        print("  Simulating human behavior...")
        time.sleep(random.uniform(5, 8))
        
        # Random scrolling
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 1500);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(3)
        
        # Wait for content
        print("  Waiting for content to load...")
        time.sleep(15)
        
        # Get HTML
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Find all PDF links
        all_links = soup.find_all('a', href=True)
        print(f"    Found {len(all_links)} total links")
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if '.pdf' in href.lower():
                full_url = urljoin(url, href)
                
                # Extract issuance number for filename
                issuance_num = extract_issuance_number(text, href)
                
                if issuance_num:
                    filename = f"{issuance_num}.pdf"
                else:
                    # Fallback to original filename
                    filename = os.path.basename(urlparse(full_url).path.split('?')[0])
                
                # Avoid duplicates
                if not any(pdf['url'] == full_url for pdf in all_pdfs):
                    all_pdfs.append({
                        'url': full_url,
                        'name': text if text else issuance_num,
                        'filename': filename,
                        'issuance': issuance_num,
                        'referer': url
                    })
                    print(f"      ✓ {text[:60]}... -> {filename}")
        
        print(f"    Total PDFs found: {len(all_pdfs)}")
        return all_pdfs
        
    except Exception as e:
        print(f"    ✗ Error extracting PDFs: {str(e)}")
        return []
        
    finally:
        driver.quit()


def scrape_dod_publications(category_urls, output_dir):
    """
    Main function to scrape DoD publications from provided category URLs.
    """
    all_pdfs = []
    
    print(f"\n{'='*60}")
    print(f"Starting scrape of {len(category_urls)} DoD categories")
    print(f"{'='*60}")
    
    # Process each category
    for cat_idx, (category_url, category_name) in enumerate(category_urls.items(), 1):
        print(f"\n{'='*60}")
        print(f"Processing Category {cat_idx}/{len(category_urls)}: {category_name}")
        print(f"{'='*60}")
        
        # Extract PDFs from category
        pdfs = extract_dod_pdfs(category_url, category_name)
        
        if not pdfs:
            print(f"  ✗ No PDFs found in category: {category_name}")
            continue
        
        # Add category to each PDF
        for pdf in pdfs:
            pdf['category'] = category_name
            all_pdfs.append(pdf)
        
        print(f"\n  ✓ Found {len(pdfs)} PDFs in {category_name}")
        
        # Human-like delay between categories
        if cat_idx < len(category_urls):
            delay = random.uniform(10, 15)
            print(f"\n  Waiting {delay:.1f}s before next category...")
            time.sleep(delay)
    
    return all_pdfs


def download_pdfs_with_selenium(pdf_list, output_dir):
    """
    Download PDFs using Selenium to maintain browser session context.
    This method navigates to each PDF URL with Selenium and saves the content.
    """
    print("\n🔧 Using Selenium-based download (maintains browser session)")
    print("="*60 + "\n")
    
    for idx, pdf in enumerate(pdf_list, 1):
        driver = None
        try:
            # Create category subdirectory
            category_dir = os.path.join(output_dir, pdf.get('category', 'uncategorized'))
            if not os.path.exists(category_dir):
                os.makedirs(category_dir)
            
            filename = pdf['filename']
            filepath = os.path.join(category_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(pdf_list)}] ⊙ {filename} (already exists)")
                continue
            
            print(f"[{idx}/{len(pdf_list)}] ↓ {filename}")
            
            # Create temporary download directory
            temp_download_dir = os.path.join(output_dir, '_temp_downloads')
            if not os.path.exists(temp_download_dir):
                os.makedirs(temp_download_dir)
            
            # Set up browser with download preferences
            driver = setup_realistic_browser(download_dir=temp_download_dir)
            
            # CRITICAL: Visit the referer page first to establish session
            if 'referer' in pdf:
                print(f"      → Visiting category page first...")
                driver.get(pdf['referer'])
                time.sleep(random.uniform(3, 5))
            
            # Now navigate to the PDF URL
            print(f"      → Downloading PDF...")
            driver.get(pdf['url'])
            
            # Wait for download to complete
            time.sleep(8)
            
            # Check if file was downloaded to temp directory
            downloaded_files = os.listdir(temp_download_dir)
            pdf_files = [f for f in downloaded_files if f.endswith('.pdf') and not f.endswith('.crdownload')]
            
            if pdf_files:
                # Move the downloaded file to the correct location with correct name
                source_file = os.path.join(temp_download_dir, pdf_files[0])
                shutil.move(source_file, filepath)
                file_size = os.path.getsize(filepath)
                print(f"      ✓ {file_size / 1024:.2f} KB")
            else:
                print(f"      ⚠ Download may have failed or is still in progress")
            
            # Clean up temp directory
            for f in os.listdir(temp_download_dir):
                try:
                    os.remove(os.path.join(temp_download_dir, f))
                except:
                    pass
            
            # Delay between downloads
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"      ✗ {str(e)}")
        finally:
            if driver:
                driver.quit()
    
    # Clean up temp directory
    try:
        temp_download_dir = os.path.join(output_dir, '_temp_downloads')
        if os.path.exists(temp_download_dir):
            shutil.rmtree(temp_download_dir)
    except:
        pass


if __name__ == "__main__":
    print("DoD Publications PDF Scraper - SELENIUM DOWNLOAD MODE")
    print("="*60)
    print("✓ Using Selenium to download PDFs (bypasses 403 errors)")
    print("✓ Browser will run in BACKGROUND - your computer remains usable!")
    print("="*60 + "\n")
    
    # DoD category URLs with descriptive names
    category_urls = {
        'https://www.esd.whs.mil/Directives/issuances/dodd/': 'DoD_Directives',
        'https://www.esd.whs.mil/Directives/issuances/dodi/': 'DoD_Instructions',
        'https://www.esd.whs.mil/Directives/issuances/dodm/': 'DoD_Manuals',
        'https://www.esd.whs.mil/Directives/issuances/admin_inst/': 'Admin_Instructions',
        'https://www.esd.whs.mil/DD/DoD-Issuances/DTM/': 'DTM',
        'https://www.esd.whs.mil/DD/DoD-Issuances/140025/': 'Series_140025'
    }
    
    output_dir = 'dod_pdfs'
    
    start_time = datetime.datetime.now()
    
    print("⚠️  FULL PRODUCTION MODE - SELENIUM DOWNLOADS")
    print("="*60)
    print(f"  Categories: {len(category_urls)} (ALL)")
    print(f"  Output: {output_dir}")
    print(f"  Start Time: {start_time.strftime('%I:%M:%S %p')}")
    print(f"  ⏱️  Estimated: 2-4 hours (Selenium downloads are slower but reliable)")
    print(f"  💻 Your computer will remain fully usable!")
    print(f"  🔧 Selenium maintains browser session to avoid 403 errors")
    print("="*60 + "\n")
    
    proceed = input("🚀 Start FULL headless scraping with Selenium downloads? (yes/no): ").strip().lower()
    
    if proceed not in ['yes', 'y']:
        print("\n❌ Scraping cancelled.")
        exit(0)
    
    print("\n🚀 Starting FULL headless scrape...")
    print("💡 TIP: You can continue using your computer normally!")
    print("="*60 + "\n")
    
    # Scrape ALL publications
    pdfs = scrape_dod_publications(category_urls, output_dir)
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}\n")
    print(f"⏱️  Duration: {duration}")
    print(f"🕐 Start: {start_time.strftime('%I:%M:%S %p')}")
    print(f"🕐 End: {end_time.strftime('%I:%M:%S %p')}\n")
    
    if pdfs:
        categories = {}
        for pdf in pdfs:
            cat = pdf.get('category', 'uncategorized')
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(pdf)
        
        print(f"✅ Total PDFs found: {len(pdfs)}")
        print(f"📁 Categories: {len(categories)}\n")
        
        for cat, cat_pdfs in sorted(categories.items()):
            print(f"  ✓ {cat}: {len(cat_pdfs)} PDFs")
        
        download = input(f"\n💾 Download all {len(pdfs)} PDFs using Selenium? (yes/no): ").strip().lower()
        if download in ['yes', 'y']:
            print(f"\n{'='*60}")
            print("DOWNLOADING PDFs WITH SELENIUM")
            print(f"{'='*60}\n")
            download_pdfs_with_selenium(pdfs, output_dir)
            print("\n✅ Download complete!")
            print(f"📂 Saved to: {os.path.abspath(output_dir)}")
        else:
            print("\n❌ Download cancelled.")
    else:
        print("✗ No PDFs found")