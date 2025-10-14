import os
import time
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
    print("Install required packages: pip install selenium beautifulsoup4")
    exit(1)


def extract_all_pdfs_from_table(output_dir='af_publications'):
    """
    Load the page with Selenium, extract PDFs directly from the rendered table.
    Navigate through all 178 pages.
    """
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    all_pdfs = []
    
    try:
        url = "https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=10141&catID=1&series=-1&modID=449&tabID=131"
        
        print("Loading Air Force e-publishing page...")
        driver.get(url)
        
        # Wait for table to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "data"))
        )
        
        # Trigger the data load
        print("Triggering data load...")
        driver.execute_script("epubs.SelectOrg(10141, 1, false, 'Departmental', 449, 131);")
        time.sleep(10)
        
        # Get total pages
        try:
            info = driver.find_element(By.CLASS_NAME, "dataTables_info").text
            print(f"Table info: {info}")
            
            # Try to find last page number
            pagination = driver.find_elements(By.CSS_SELECTOR, ".paginate_button")
            page_numbers = []
            for button in pagination:
                try:
                    text = button.text.strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                except:
                    continue
            
            total_pages = max(page_numbers) if page_numbers else 178
            print(f"Total pages to process: {total_pages}\n")
            
        except:
            total_pages = 178
            print(f"Defaulting to 178 pages\n")
        
        # Process each page
        for page_num in range(1, total_pages + 1):
            print(f"[Page {page_num}/{total_pages}] Extracting PDFs...")
            
            # Wait for page to stabilize
            time.sleep(3)
            
            # Get page HTML
            page_html = driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')
            
            # Find the data table
            table = soup.find('table', {'id': 'data'})
            if table:
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if not cells:
                            continue
                        
                        # First cell has product number
                        product_num = cells[0].get_text(strip=True)
                        
                        # Find all PDF links in the row
                        for link in row.find_all('a', href=True):
                            href = link['href']
                            if '.pdf' in href.lower():
                                full_url = urljoin(url, href)
                                
                                # Avoid duplicates
                                if not any(pdf['url'] == full_url for pdf in all_pdfs):
                                    all_pdfs.append({
                                        'url': full_url,
                                        'product': product_num
                                    })
                                    print(f"  ✓ {product_num}")
            
            # Navigate to next page
            if page_num < total_pages:
                try:
                    # Try JavaScript navigation first
                    driver.execute_script("$('#data').DataTable().page('next').draw('page');")
                    time.sleep(2)
                except:
                    # Fallback: click next button
                    try:
                        next_btn = driver.find_element(By.ID, "data_next")
                        if "disabled" not in next_btn.get_attribute("class"):
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(2)
                    except Exception as e:
                        print(f"  ⚠ Navigation failed: {str(e)}")
                        break
        
        print(f"\n{'='*60}")
        print(f"✓ Extraction Complete!")
        print(f"Total PDFs found: {len(all_pdfs)}")
        print(f"{'='*60}\n")
        
        return all_pdfs
        
    finally:
        driver.quit()


def download_pdfs(pdf_list, output_dir='af_publications'):
    """Download all PDFs from the list."""
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.e-publishing.af.mil/'
    })
    
    successful = 0
    failed = 0
    
    for idx, pdf in enumerate(pdf_list, 1):
        try:
            filename = os.path.basename(pdf['url'].split('?')[0])
            filepath = os.path.join(output_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(pdf_list)}] ⊙ {pdf['product']}")
                successful += 1
                continue
            
            print(f"[{idx}/{len(pdf_list)}] ↓ {pdf['product']} - {filename}")
            
            response = session.get(pdf['url'], timeout=60)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"  ✓ {len(response.content) / 1024:.2f} KB")
            successful += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  ✗ {str(e)}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"Download Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(pdf_list)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    print("Air Force E-Publishing - Table Extraction Method")
    print("="*60 + "\n")
    
    output_dir = input("Output directory (default 'af_publications'): ").strip() or 'af_publications'
    
    # Extract PDFs from all pages
    pdf_list = extract_all_pdfs_from_table(output_dir)
    
    if pdf_list:
        # Download all PDFs
        download_pdfs(pdf_list, output_dir)
    else:
        print("✗ No PDFs found")