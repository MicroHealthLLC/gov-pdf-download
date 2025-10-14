import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import logging
import json
from datetime import datetime
import asyncio
import subprocess
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("va_vdl_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class VAVDLScraper:
    def __init__(self, output_dir="va_vdl_software"):
        self.output_dir = output_dir
        
        # VA VDL (Veterans Document Library) category URLs
        self.category_urls = {
            'https://www.va.gov/vdl/section.asp?secid=1': 'Clinical_Section',
            'https://www.va.gov/vdl/section.asp?secid=2': 'Infrastructure_Section',
            'https://www.va.gov/vdl/section.asp?secid=3': 'Business_Section',
            'https://www.va.gov/vdl/section.asp?secid=4': 'Administrative_Section',
            'https://www.va.gov/vdl/section.asp?secid=6': 'Security_Section'
        }
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Add tracking file for resuming downloads
        self.tracking_file = os.path.join(self.output_dir, "download_tracking.json")
        self.downloaded_files = self._load_tracking()
        
        # Playwright objects
        self.playwright = None
        self.browser = None
        self.context = None
        
    def _load_tracking(self):
        """Load tracking information from file"""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_tracking(self, doc_url, success):
        """Save tracking information to file"""
        self.downloaded_files[doc_url] = {
            'timestamp': datetime.now().isoformat(),
            'success': success
        }
        try:
            with open(self.tracking_file, 'w') as f:
                json.dump(self.downloaded_files, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving tracking info: {e}")
    
    async def initialize_browser(self):
        """Initialize Playwright browser"""
        if not self.playwright:
            self.playwright = await async_playwright().start()
            
            self.browser = await self.playwright.chromium.launch(
                headless=True,
            )
            
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True,
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "max-age=0",
                    "DNT": "1"
                }
            )
            
            logger.info("Playwright browser initialized successfully")
    
    async def close_browser(self):
        """Close Playwright browser"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
            
        self.context = None
        self.browser = None
        self.playwright = None
        
        logger.info("Playwright browser closed")
    
    async def get_page_content(self, url):
        """Get HTML content of a page using Playwright"""
        if not self.browser:
            await self.initialize_browser()
            
        try:
            page = await self.context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)
            content = await page.content()
            await page.close()
            return content
        except Exception as e:
            logger.error(f"Error getting page content for {url}: {e}")
            return None
    
    async def extract_application_links(self, category_url, category_name):
        """Extract application links from a VDL section page"""
        app_links = []
        content = await self.get_page_content(category_url)
        
        if not content:
            logger.error(f"Failed to get content from {category_url}")
            return app_links
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Find all links - VDL uses links for application names
        all_links = soup.select('a[href*="application.asp"]')
        
        found_links = set()
        
        for link in all_links:
            href = link.get('href')
            text = link.get_text(strip=True)
            
            if href and text:
                # Skip very short text
                if len(text) < 3:
                    continue
                
                full_url = urljoin(category_url, href)
                
                # Avoid duplicates
                if full_url not in found_links:
                    found_links.add(full_url)
                    app_links.append({
                        'url': full_url,
                        'name': text,
                        'category': category_name,
                        'referer': category_url
                    })
                    logger.info(f"Found application: {text}")
        
        return app_links
    
    async def extract_documents_from_application(self, app_url, app_name, category_name, referer):
        """Extract document links (PDF and DOC) from an application page"""
        documents = []
        content = await self.get_page_content(app_url)
        
        if not content:
            logger.error(f"Failed to get content from {app_url}")
            return documents
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Find the table containing documents
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row_idx, row in enumerate(rows):
                cells = row.find_all('td')
                
                if len(cells) < 3:  # Need at least Name, Date, Document columns
                    continue
                
                # Extract document name (usually first column)
                doc_name = cells[0].get_text(strip=True)
                if not doc_name or len(doc_name) < 3:
                    continue
                
                # Skip header rows
                if 'name' in doc_name.lower() and row_idx == 0:
                    continue
                
                # Look for PDF and DOC links in the "Document" column (usually last column)
                document_cell = cells[-1]
                
                links = document_cell.find_all('a', href=True)
                
                row_pdf = None
                row_doc = None
                
                for link in links:
                    href = link.get('href')
                    link_text = link.get_text(strip=True)
                    
                    # Check if it's a direct PDF or DOC link
                    if '.pdf' in href.lower():
                        full_url = urljoin(app_url, href)
                        row_pdf = {
                            'url': full_url,
                            'name': doc_name,
                            'type': 'PDF',
                            'app_name': app_name,
                            'category': category_name,
                            'referer': app_url,
                            'section_referer': referer
                        }
                    elif '.doc' in href.lower() or 'docx' in href.lower():
                        full_url = urljoin(app_url, href)
                        row_doc = {
                            'url': full_url,
                            'name': doc_name,
                            'type': 'DOC',
                            'app_name': app_name,
                            'category': category_name,
                            'referer': app_url,
                            'section_referer': referer
                        }
                
                # Prefer PDF over DOC
                if row_pdf:
                    documents.append(row_pdf)
                    logger.info(f"  → Found PDF: {row_pdf['name']}")
                elif row_doc:
                    documents.append(row_doc)
                    logger.info(f"  → Found DOC (no PDF available): {row_doc['name']}")
        
        return documents
    
    def generate_filename(self, doc_info):
        """Generate a safe filename for the document"""
        app_name = doc_info.get('app_name', 'Unknown')
        doc_name = doc_info.get('name', 'document')
        doc_type = doc_info.get('type', 'PDF')
        
        # Clean up names
        app_name_clean = re.sub(r'[^\w\s-]', '', app_name)[:30]
        doc_name_clean = re.sub(r'[^\w\s-]', '', doc_name)[:50]
        
        # Create filename
        filename = f"{app_name_clean}_{doc_name_clean}.{doc_type.lower()}"
        filename = filename.replace(' ', '_')
        
        return filename
    
    async def download_with_curl(self, doc_url, filepath, referer):
        """Download document using curl as a fallback method"""
        try:
            logger.info("      → Trying curl download method...")
            
            curl_command = [
                'curl',
                '-L',
                '-o', filepath,
                '-H', f'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                '-H', f'Referer: {referer}',
                '-H', 'Accept: application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document,*/*',
                '-H', 'Accept-Language: en-US,en;q=0.9',
                '-H', 'Connection: keep-alive',
                '--compressed',
                '--max-time', '60',
                doc_url
            ]
            
            result = subprocess.run(curl_command, capture_output=True, text=True, timeout=70)
            
            if result.returncode == 0 and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                logger.info("      ✓ Successfully downloaded with curl")
                return True
            else:
                logger.warning(f"      ⚠ Curl failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"      ✗ Curl download error: {e}")
            return False
    
    async def download_with_playwright(self, doc_info, filepath):
        """Download document using Playwright - direct PDF/DOC links"""
        if not self.browser:
            await self.initialize_browser()
            
        try:
            page = await self.context.new_page()
            
            # Visit the section page first
            section_referer = doc_info.get('section_referer')
            if section_referer:
                logger.info(f"      → Visiting section page first...")
                await page.goto(section_referer, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)
            
            # Visit the application page
            app_referer = doc_info.get('referer')
            if app_referer:
                logger.info(f"      → Visiting application page...")
                await page.goto(app_referer, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)
            
            doc_url = doc_info['url']
            
            # Set up download listener
            download_event = None
            download_completed = False
            
            async def handle_download(download):
                nonlocal download_event, download_completed
                download_event = download
                try:
                    await download.save_as(filepath)
                    download_completed = True
                    logger.info(f"      ✓ Download completed via event handler")
                except Exception as e:
                    logger.error(f"      ✗ Error saving download: {e}")
            
            page.on("download", handle_download)
            
            # Navigate to document URL - this is a direct PDF/DOC link
            logger.info(f"      → Downloading from: {doc_url}")
            try:
                await page.goto(doc_url, wait_until="commit", timeout=60000)
                await page.wait_for_timeout(5000)
                
                if download_completed and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                    await page.close()
                    return True
                
            except Exception as nav_error:
                if "Download is starting" in str(nav_error):
                    logger.info(f"      → Download triggered immediately (expected)")
                    await page.wait_for_timeout(10000)
                else:
                    raise nav_error
            
            # Check if download completed
            if download_completed and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                await page.close()
                return True
            
            # Wait longer if download event occurred
            if download_event and not download_completed:
                logger.info(f"      → Waiting for download to complete...")
                await page.wait_for_timeout(10000)
                
                if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                    await page.close()
                    return True
            
            # Try direct content extraction as fallback
            if not os.path.exists(filepath) or os.path.getsize(filepath) < 1000:
                logger.info("      → Trying direct content extraction...")
                
                await page.close()
                page = await self.context.new_page()
                
                if app_referer:
                    await page.goto(app_referer, wait_until="networkidle", timeout=60000)
                    await page.wait_for_timeout(2000)
                
                pdf_content = await page.evaluate("""
                    async (url) => {
                        try {
                            const response = await fetch(url, {
                                method: 'GET',
                                credentials: 'include',
                                headers: {
                                    'Accept': 'application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                    'Referer': document.referrer || window.location.origin
                                }
                            });
                            
                            if (!response.ok) {
                                return null;
                            }
                            
                            const arrayBuffer = await response.arrayBuffer();
                            const uint8Array = new Uint8Array(arrayBuffer);
                            let binaryString = '';
                            uint8Array.forEach(byte => {
                                binaryString += String.fromCharCode(byte);
                            });
                            return btoa(binaryString);
                        } catch (error) {
                            console.error('Error fetching document:', error);
                            return null;
                        }
                    }
                """, doc_url)
                
                if pdf_content:
                    import base64
                    with open(filepath, 'wb') as f:
                        f.write(base64.b64decode(pdf_content))
                    
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                        logger.info("      ✓ Successfully downloaded using direct content extraction")
                        await page.close()
                        return True
            
            await page.close()
            
            # Final check
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                logger.info("      ✓ Successfully downloaded document")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"      ✗ Error downloading with Playwright: {e}")
            return False
    
    async def download_single_document(self, doc_info):
        """Download a single document with tracking and multiple retry strategies"""
        doc_url = doc_info['url']
        
        # Check if we've already tried this URL
        if doc_url in self.downloaded_files:
            if self.downloaded_files[doc_url]['success']:
                logger.info(f"Already successfully downloaded: {doc_url}")
                return True
            else:
                logger.info(f"Previously failed, retrying: {doc_url}")
        
        # Generate filename and create category subdirectory
        category = doc_info.get('category', 'uncategorized')
        category_dir = os.path.join(self.output_dir, category)
        os.makedirs(category_dir, exist_ok=True)
        
        filename = self.generate_filename(doc_info)
        filepath = os.path.join(category_dir, filename)
        
        # Skip if file already exists and is valid
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            logger.info(f"File already exists: {filepath}")
            self._save_tracking(doc_url, True)
            return True
        
        # Try with multiple retries and methods
        max_retries = 3
        for retry in range(max_retries):
            try:
                logger.info(f"Downloading document, attempt {retry+1}/{max_retries} for {filename}...")
                
                # Try Playwright first
                success = await self.download_with_playwright(doc_info, filepath)
                
                if success:
                    file_size = os.path.getsize(filepath)
                    logger.info(f"      ✓ Successfully downloaded {filename} ({file_size / 1024:.2f} KB)")
                    self._save_tracking(doc_url, True)
                    return True
                
                # If Playwright fails, try curl as fallback on last attempt
                if retry == max_retries - 1:
                    logger.info("      → Playwright failed, trying curl as final attempt...")
                    success = await self.download_with_curl(doc_url, filepath, doc_info.get('referer'))
                    if success:
                        file_size = os.path.getsize(filepath)
                        logger.info(f"      ✓ Successfully downloaded with curl {filename} ({file_size / 1024:.2f} KB)")
                        self._save_tracking(doc_url, True)
                        return True
                
                logger.warning(f"      ⚠ Download failed for {filename}, attempt {retry+1}")
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"      ✗ Error downloading {filename}: {e}")
                await asyncio.sleep(3)
        
        # If all retries fail
        logger.error(f"All download attempts failed for {filename}")
        self._save_tracking(doc_url, False)
        return False
    
    async def process_vdl_sections(self, selected_categories=None):
        """Process VA VDL sections from selected categories"""
        try:
            await self.initialize_browser()
            
            # Determine which categories to process
            if selected_categories:
                categories_to_process = {k: v for k, v in self.category_urls.items() if v in selected_categories}
            else:
                categories_to_process = self.category_urls
            
            total_categories = len(categories_to_process)
            logger.info(f"Processing {total_categories} VA VDL categories...")
            
            # Process each category
            for cat_idx, (category_url, category_name) in enumerate(categories_to_process.items(), 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing Category {cat_idx}/{total_categories}: {category_name}")
                logger.info(f"{'='*60}")
                
                # Extract application links from the section page
                app_links = await self.extract_application_links(category_url, category_name)
                
                if not app_links:
                    logger.warning(f"No applications found in category: {category_name}")
                    continue
                
                logger.info(f"Found {len(app_links)} applications in {category_name}")
                
                # Process each application
                for app_idx, app_info in enumerate(app_links, 1):
                    logger.info(f"\n[Application {app_idx}/{len(app_links)}] {app_info['name']}")
                    
                    # Extract documents from this application
                    documents = await self.extract_documents_from_application(
                        app_info['url'],
                        app_info['name'],
                        category_name,
                        app_info['referer']
                    )
                    
                    if not documents:
                        logger.warning(f"  No documents found for application: {app_info['name']}")
                        continue
                    
                    logger.info(f"  Found {len(documents)} documents")
                    
                    # Download each document
                    for doc_idx, doc_info in enumerate(documents, 1):
                        logger.info(f"\n  [{doc_idx}/{len(documents)}] Downloading: {doc_info['name'][:60]}...")
                        await self.download_single_document(doc_info)
                        await asyncio.sleep(2)
                    
                    # Delay between applications
                    await asyncio.sleep(3)
                
                # Add a delay between categories
                if cat_idx < total_categories:
                    delay = 5
                    logger.info(f"\nWaiting {delay}s before next category...")
                    await asyncio.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("Process interrupted by user. Progress saved, you can resume later.")
        except Exception as e:
            logger.error(f"Error in processing: {e}")
        finally:
            await self.close_browser()
            logger.info("Processing completed.")

async def main():
    """Main function to run the VA VDL scraper"""
    scraper = VAVDLScraper()
    
    print("\n" + "="*60)
    print("VA Software Document Library (VDL) Scraper")
    print("="*60)
    print("This script will download software documentation from VA VDL.")
    print("It will download PDFs when available, otherwise DOC files.")
    print("Progress is tracked, so you can resume if interrupted.")
    print("="*60 + "\n")
    
    # Display available categories
    print("Available VA VDL Sections:")
    for idx, (url, name) in enumerate(scraper.category_urls.items(), 1):
        print(f"  {idx}. {name}")
    
    print("\n" + "="*60)
    category_choice = input("\nProcess all sections or select specific ones? (all/select): ").strip().lower()
    
    selected_categories = None
    if category_choice == 'select':
        print("\nEnter section numbers separated by commas (e.g., 1,3,5):")
        selections = input("Sections: ").strip()
        try:
            indices = [int(x.strip()) for x in selections.split(',')]
            category_names = list(scraper.category_urls.values())
            selected_categories = [category_names[i-1] for i in indices if 0 < i <= len(category_names)]
            print(f"\nSelected sections: {', '.join(selected_categories)}")
        except:
            print("Invalid selection. Processing all sections.")
            selected_categories = None
    
    print("\n" + "="*60)
    print("Starting VA VDL scraper...")
    print("Press Ctrl+C at any time to stop. Progress will be saved.")
    print("="*60 + "\n")
    
    await scraper.process_vdl_sections(selected_categories)
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETED")
    print("="*60)
    print(f"Downloaded files are saved in: {os.path.abspath(scraper.output_dir)}")
    print(f"Check the log file for details: va_vdl_scraper.log")
    print("="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(main())