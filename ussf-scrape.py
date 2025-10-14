import os
import time
from urllib.parse import urljoin, urlparse, parse_qs
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


def extract_url_params(url):
    """Extract orgID and catID from URL fragment."""
    # Parse fragment after #/
    if '#/' in url:
        fragment = url.split('#/')[1]
        params = {}
        for param in fragment.split('&'):
            if '=' in param:
                key, value = param.split('=')
                params[key] = value
        return params.get('orgID'), params.get('catID')
    return None, None


def extract_all_pdfs_from_table(url, org_name, output_dir='space_force_publications'):
    """
    Load the page with Selenium, extract PDFs directly from the rendered table.
    Navigate through all pages for a given organization.
    """
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    all_pdfs = []
    
    try:
        print(f"\n{'='*60}")
        print(f"Processing: {org_name}")
        print(f"{'='*60}")
        print(f"Loading page: {url}")
        
        driver.get(url)
        
        # Wait for table to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "data"))
        )
        
        # Extract parameters and trigger data load
        org_id, cat_id = extract_url_params(url)
        if org_id and cat_id:
            print(f"Triggering data load for orgID={org_id}, catID={cat_id}...")
            driver.execute_script(f"epubs.SelectOrg({org_id}, {cat_id}, false, 'Departmental', 449, 131);")
            time.sleep(10)
        
        # Get total pages
        try:
            info = driver.find_element(By.CLASS_NAME, "dataTables_info").text
            print(f"Table info: {info}")
            
            pagination = driver.find_elements(By.CSS_SELECTOR, ".paginate_button")
            page_numbers = []
            for button in pagination:
                try:
                    text = button.text.strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                except:
                    continue
            
            total_pages = max(page_numbers) if page_numbers else 1
            print(f"Total pages to process: {total_pages}\n")
            
        except:
            total_pages = 1
            print(f"Defaulting to 1 page\n")
        
        # Process each page
        for page_num in range(1, total_pages + 1):
            print(f"[Page {page_num}/{total_pages}] Extracting PDFs...")
            
            time.sleep(3)
            
            page_html = driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')
            
            table = soup.find('table', {'id': 'data'})
            if table:
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if not cells:
                            continue
                        
                        product_num = cells[0].get_text(strip=True)
                        
                        for link in row.find_all('a', href=True):
                            href = link['href']
                            if '.pdf' in href.lower():
                                full_url = urljoin(url, href)
                                
                                if not any(pdf['url'] == full_url for pdf in all_pdfs):
                                    all_pdfs.append({
                                        'url': full_url,
                                        'product': product_num,
                                        'org': org_name
                                    })
                                    print(f"  ✓ {product_num}")
            
            # Navigate to next page
            if page_num < total_pages:
                try:
                    driver.execute_script("$('#data').DataTable().page('next').draw('page');")
                    time.sleep(2)
                except:
                    try:
                        next_btn = driver.find_element(By.ID, "data_next")
                        if "disabled" not in next_btn.get_attribute("class"):
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(2)
                    except Exception as e:
                        print(f"  ⚠ Navigation failed: {str(e)}")
                        break
        
        print(f"\n✓ {org_name} Complete! PDFs found: {len(all_pdfs)}")
        return all_pdfs
        
    finally:
        driver.quit()


def download_pdfs(pdf_list, output_dir='space_force_publications'):
    """Download all PDFs from the list, organized by organization."""
    
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
            # Create org-specific subdirectory
            org_dir = os.path.join(output_dir, pdf['org'].replace(' ', '_'))
            if not os.path.exists(org_dir):
                os.makedirs(org_dir)
            
            filename = os.path.basename(pdf['url'].split('?')[0])
            filepath = os.path.join(org_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(pdf_list)}] ⊙ {pdf['org']} - {pdf['product']}")
                successful += 1
                continue
            
            print(f"[{idx}/{len(pdf_list)}] ↓ {pdf['org']} - {pdf['product']} - {filename}")
            
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
    print("Space Force E-Publishing - Multi-Organization Extraction")
    print("="*60 + "\n")
    
    # Define Space Force URLs with friendly names
    space_force_urls = [
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16498&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16498"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16287&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16287"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16330&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16330"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16495&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16495"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=47&catID=20&series=-1&modID=449&tabID=131", "SF_Org_47"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16297&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16297"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16481&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16481"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16023&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16023"),
        ("https://www.e-publishing.af.mil/Product-Index/#/?view=pubs&orgID=16254&catID=20&series=-1&modID=449&tabID=131", "SF_Org_16254"),
    ]
    
    output_dir = input("Output directory (default 'space_force_publications'): ").strip() or 'space_force_publications'
    
    all_pdfs = []
    
    # Extract PDFs from all organizations
    for url, org_name in space_force_urls:
        pdfs = extract_all_pdfs_from_table(url, org_name, output_dir)
        all_pdfs.extend(pdfs)
    
    print(f"\n{'='*60}")
    print(f"✓ ALL EXTRACTIONS COMPLETE!")
    print(f"Total PDFs found across all organizations: {len(all_pdfs)}")
    print(f"{'='*60}\n")
    
    if all_pdfs:
        download_pdfs(all_pdfs, output_dir)
    else:
        print("✗ No PDFs found")