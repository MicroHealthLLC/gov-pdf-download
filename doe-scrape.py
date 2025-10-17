from playwright.sync_api import sync_playwright
import os
import time
import re
import requests
from datetime import datetime

class DOEScraperPlaywright:
    def __init__(self, output_dir='doe_documents'):
        """
        Initialize DOE scraper using Playwright
        
        Args:
            output_dir (str): Directory to save downloaded PDFs
        """
        self.base_url = 'https://www.directives.doe.gov'
        self.urls = {
            'directives': f'{self.base_url}/directives-browse',
            'guidance': f'{self.base_url}/guidance'
        }
        self.output_dir = output_dir
        
        # Create output directories
        self.directives_dir = os.path.join(output_dir, 'directives')
        self.guidance_dir = os.path.join(output_dir, 'guidance')
        
        for directory in [self.output_dir, self.directives_dir, self.guidance_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
        
        # Setup logging
        self.log_file = os.path.join(self.output_dir, 'scraping_log.txt')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def log(self, message):
        """Write message to log file and print to console"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - {message}"
        print(log_message)
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"{log_message}\n")
    
    def scrape_page(self, page, url, doc_type):
        """
        Scrape a specific DOE page for documents
        
        Args:
            page: Playwright page object
            url (str): URL to scrape
            doc_type (str): Type of documents ('directives' or 'guidance')
            
        Returns:
            list: List of document information tuples
        """
        documents = []
        
        try:
            self.log(f"Loading {doc_type} page: {url}")
            page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for table to be visible
            page.wait_for_selector('table', timeout=30000)
            
            # Additional wait for dynamic content
            time.sleep(3)
            
            # Get all table rows
            rows = page.query_selector_all('table tbody tr')
            
            if not rows:
                # Try alternative selector
                rows = page.query_selector_all('table tr')
                # Skip header row
                if rows:
                    rows = rows[1:]
            
            self.log(f"Found {len(rows)} rows in {doc_type} table")
            
            for idx, row in enumerate(rows):
                try:
                    cells = row.query_selector_all('td')
                    
                    if len(cells) >= 3:
                        # Column 0: Document ID
                        doc_id = cells[0].inner_text().strip()
                        
                        # Column 1: Title with link
                        link_element = cells[1].query_selector('a')
                        if link_element:
                            doc_url = link_element.get_attribute('href')
                            if doc_url and not doc_url.startswith('http'):
                                doc_url = f"{self.base_url}{doc_url}"
                            title = link_element.inner_text().strip()
                            
                            # Column 2: Type
                            category = cells[2].inner_text().strip()
                            
                            if doc_url and title:
                                documents.append((doc_id, doc_url, title, category, doc_type))
                                if idx < 5:  # Log first 5 for verification
                                    self.log(f"  Found: {doc_id} - {title[:50]}...")
                
                except Exception as e:
                    self.log(f"  Error parsing row: {e}")
                    continue
            
            self.log(f"Total {doc_type} found: {len(documents)}")
            return documents
            
        except Exception as e:
            self.log(f"Error scraping {doc_type} page: {e}")
            import traceback
            self.log(traceback.format_exc())
            return []
    
    def download_document(self, doc_id, doc_url, title, category, doc_type):
        """
        Download PDF document
        
        Args:
            doc_id (str): Document identifier
            doc_url (str): URL to document detail page
            title (str): Document title
            category (str): Document category/type
            doc_type (str): 'directives' or 'guidance'
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Construct PDF download URL
        pdf_url = f"{doc_url}/@@images/file"
        
        # Determine output directory
        output_dir = self.directives_dir if doc_type == 'directives' else self.guidance_dir
        
        # Create safe filename
        safe_id = re.sub(r'[<>:"/\\|?*]', '_', doc_id)
        safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)
        safe_category = re.sub(r'[<>:"/\\|?*]', '_', category)
        
        filename = f"{safe_id}_{safe_category}_{safe_title[:80]}.pdf"
        filepath = os.path.join(output_dir, filename)
        
        # Skip if already exists
        if os.path.exists(filepath):
            self.log(f"⊙ Already exists: {filename}")
            return True
        
        # Download with retry logic
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.log(f"Downloading: {doc_id} - {title[:50]}... (Attempt {attempt})")
                
                response = self.session.get(pdf_url, stream=True, timeout=60)
                response.raise_for_status()
                
                # Verify content type
                content_type = response.headers.get('content-type', '').lower()
                if 'pdf' not in content_type and 'octet-stream' not in content_type:
                    self.log(f"⚠ Warning: Content type is {content_type}")
                
                # Save file
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                file_size = os.path.getsize(filepath)
                
                # Verify it's actually a PDF (check magic bytes)
                with open(filepath, 'rb') as f:
                    header = f.read(4)
                    if header != b'%PDF':
                        self.log(f"⚠ Warning: File doesn't appear to be a PDF")
                
                self.log(f"✓ Saved: {filename} ({file_size:,} bytes)")
                return True
                
            except requests.exceptions.RequestException as e:
                self.log(f"✗ Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    self.log(f"  Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.log(f"✗ Failed after {max_retries} attempts: {doc_id}")
                    # Clean up partial file
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    return False
            except Exception as e:
                self.log(f"✗ Unexpected error: {e}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                return False
        
        return False
    
    def scrape_all(self, delay=2):
        """
        Scrape both directives and guidance pages
        
        Args:
            delay (int): Delay in seconds between downloads
        """
        self.log("=" * 70)
        self.log("DOE Directives and Guidance Scraper - Playwright Version")
        self.log(f"Output directory: {self.output_dir}")
        self.log("=" * 70)
        
        all_documents = []
        
        with sync_playwright() as p:
            # Launch browser
            self.log("Launching browser...")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Scrape directives
            self.log("\n" + "=" * 70)
            self.log("SCRAPING DIRECTIVES")
            self.log("=" * 70)
            directives = self.scrape_page(page, self.urls['directives'], 'directives')
            all_documents.extend(directives)
            
            # Scrape guidance
            self.log("\n" + "=" * 70)
            self.log("SCRAPING GUIDANCE DOCUMENTS")
            self.log("=" * 70)
            guidance = self.scrape_page(page, self.urls['guidance'], 'guidance')
            all_documents.extend(guidance)
            
            # Close browser
            browser.close()
        
        if not all_documents:
            self.log("\n⚠ No documents found on either page!")
            return
        
        # Download all documents
        self.log("\n" + "=" * 70)
        self.log(f"DOWNLOADING {len(all_documents)} DOCUMENTS")
        self.log("=" * 70)
        
        successful = 0
        failed = 0
        failed_list = []
        
        for i, (doc_id, doc_url, title, category, doc_type) in enumerate(all_documents, 1):
            self.log(f"\n[{i}/{len(all_documents)}] Type: {doc_type} | Category: {category}")
            
            if self.download_document(doc_id, doc_url, title, category, doc_type):
                successful += 1
            else:
                failed += 1
                failed_list.append(f"{doc_id} ({doc_type})")
            
            # Respectful delay
            if i < len(all_documents):
                time.sleep(delay)
        
        # Final summary
        self.log("\n" + "=" * 70)
        self.log("SCRAPING COMPLETE!")
        self.log("=" * 70)
        self.log(f"Total documents found: {len(all_documents)}")
        self.log(f"  - Directives: {len(directives)}")
        self.log(f"  - Guidance: {len(guidance)}")
        self.log(f"Successful downloads: {successful}")
        self.log(f"Failed downloads: {failed}")
        
        if failed_list:
            self.log(f"\nFailed documents:")
            for failed_doc in failed_list:
                self.log(f"  - {failed_doc}")
        
        self.log("=" * 70)

# Usage
if __name__ == "__main__":
    scraper = DOEScraperPlaywright(output_dir='doe_documents')
    scraper.scrape_all(delay=2)