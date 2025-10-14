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
        logging.FileHandler("va_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class VAPublicationScraper:
    def __init__(self, output_dir="va_pdfs"):
        self.output_dir = output_dir
        
        # VA category URLs with descriptive names
        self.category_urls = {
            'https://www.my.sites.va.gov/policy': 'VA_Policy',
            'https://www.va.gov/vapubs/search_action.cfm?dType=6': 'VA_Directives',
            'https://www.va.gov/vapubs/search_action.cfm?dType=8': 'VA_Designations',
            'https://www.va.gov/vapubs/search_action.cfm?dType=1': 'VA_Handbooks',
            'https://www.va.gov/vapubs/search_action.cfm?dType=2': 'VA_Manuals',
            'https://www.va.gov/vapubs/search_action.cfm?dType=5': 'VA_Publications',
            'https://www.vhapublications.va.gov/publications.cfm?Pub=8': 'VHA_Directives',
            'https://www.vhapublications.va.gov/publications.cfm?Pub=1': 'VHA_Handbooks',
            'https://www.vhapublications.va.gov/publications.cfm?Pub=2': 'VHA_Publications',
            'https://www.cem.va.gov/policies/directives.asp': 'CEM_Directives',
            'https://www.cem.va.gov/policies/handbooks.asp': 'CEM_Handbooks',
            'https://www.cem.va.gov/policies/notices.asp': 'CEM_Notices'
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
    
    def _save_tracking(self, pdf_url, success):
        """Save tracking information to file"""
        self.downloaded_files[pdf_url] = {
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
            
            # Use Chromium with specific options
            self.browser = await self.playwright.chromium.launch(
                headless=True,  # Set to False for debugging
            )
            
            # Create a persistent context with custom settings
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
            
            # Navigate to the URL with a timeout
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # Wait a bit for any dynamic content
            await page.wait_for_timeout(3000)
            
            # Get the page content
            content = await page.content()
            
            await page.close()
            return content
        except Exception as e:
            logger.error(f"Error getting page content for {url}: {e}")
            return None
    
    def extract_document_number(self, text, href):
        """
        Extract document number from link text or href for VA publications.
        """
        patterns = [
            r'(VA\s*Directive\s*\d+)',
            r'(VA\s*Handbook\s*\d+)',
            r'(VA\s*Manual\s*\d+)',
            r'(VHA\s*Directive\s*\d+\.\d+)',
            r'(VHA\s*Handbook\s*\d+\.\d+)',
            r'(VHA\s*Publication\s*\d+\.\d+)',
            r'(NCA\s*Directive\s*\d+)',
            r'(\d{8})',
            r'(\d+\.\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).replace(' ', '_')
        
        for pattern in patterns:
            match = re.search(pattern, href, re.IGNORECASE)
            if match:
                return match.group(1).replace(' ', '_')
        
        return None
    
    async def extract_pdf_links(self, category_url, category_name):
        """Extract document links from a VA category page"""
        doc_links = []
        content = await self.get_page_content(category_url)
        
        if not content:
            logger.error(f"Failed to get content from {category_url}")
            return doc_links
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for links in table rows - VA uses tables for document listings
        table_links = soup.select('table a')
        
        found_links = set()
        
        for link in table_links:
            href = link.get('href')
            text = link.get_text(strip=True)
            
            if href and text:
                # Skip navigation links
                if any(skip in text.lower() for skip in ['search', 'home', 'contact', 'help', 'login', 'sort']):
                    continue
                
                # Skip empty or very short text
                if len(text) < 5:
                    continue
                
                full_url = urljoin(category_url, href)
                
                # Extract document number for filename
                doc_num = self.extract_document_number(text, href)
                
                if doc_num:
                    filename = f"{doc_num}.pdf"
                else:
                    safe_text = re.sub(r'[^\w\s-]', '', text)[:50]
                    filename = f"{safe_text.replace(' ', '_')}.pdf"
                
                # Avoid duplicates
                if full_url not in found_links:
                    found_links.add(full_url)
                    doc_links.append({
                        'url': full_url,
                        'name': text,
                        'filename': filename,
                        'document_num': doc_num,
                        'category': category_name,
                        'referer': category_url
                    })
                    logger.info(f"Found document link: {text[:60]}...")
        
        return doc_links
    
    def generate_filename(self, pdf_info):
        """Generate a unique filename based on the PDF info"""
        filename = pdf_info.get('filename', 'document.pdf')
        
        # Clean up filename
        filename = re.sub(r'[^\w\-\.]', '_', filename)
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        return filename
    
    async def download_with_curl(self, pdf_url, filepath, referer):
        """Download PDF using curl as a fallback method"""
        try:
            logger.info("      → Trying curl download method...")
            
            # Build curl command with proper headers
            curl_command = [
                'curl',
                '-L',  # Follow redirects
                '-o', filepath,
                '-H', f'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                '-H', f'Referer: {referer}',
                '-H', 'Accept: application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                '-H', 'Accept-Language: en-US,en;q=0.9',
                '-H', 'Connection: keep-alive',
                '--compressed',
                '--max-time', '60',
                pdf_url
            ]
            
            # Execute curl
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
    
    async def find_pdf_link_on_page(self, page, doc_url):
        """Find the actual PDF download link on a document detail page"""
        try:
            # Wait for page to load
            await page.wait_for_load_state("networkidle", timeout=30000)
            
            # Common selectors for PDF download links on VA pages
            pdf_selectors = [
                'a[href$=".pdf"]',
                'a:has-text("Download")',
                'a:has-text("PDF")',
                'a:has-text("View")',
                'a[href*=".pdf"]',
                'a[href*="Portals"]',
                'a[href*="ViewPDF"]'
            ]
            
            for selector in pdf_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=5000)
                    if element:
                        pdf_href = await element.get_attribute('href')
                        if pdf_href:
                            pdf_url = urljoin(doc_url, pdf_href)
                            logger.info(f"      → Found PDF link: {pdf_url}")
                            return pdf_url
                except:
                    continue
            
            # If no explicit link found, check if the current page IS a PDF
            current_url = page.url
            if '.pdf' in current_url.lower():
                return current_url
            
            return None
            
        except Exception as e:
            logger.error(f"      ✗ Error finding PDF link: {e}")
            return None
    
    async def download_with_playwright(self, pdf_info, filepath):
        """Download PDF using Playwright with multiple strategies"""
        if not self.browser:
            await self.initialize_browser()
            
        try:
            page = await self.context.new_page()
            
            # First visit the category page to establish session
            referer = pdf_info.get('referer')
            if referer:
                logger.info(f"      → Visiting category page first...")
                await page.goto(referer, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)
            
            doc_url = pdf_info['url']
            
            # Set up download listener BEFORE navigating
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
            
            # Try to navigate - this might trigger immediate download
            logger.info(f"      → Navigating to document page...")
            try:
                # Use "commit" wait state instead of "networkidle" to handle immediate downloads
                await page.goto(doc_url, wait_until="commit", timeout=60000)
                
                # Wait a bit to see if download started
                await page.wait_for_timeout(3000)
                
                # If download already completed, we're done
                if download_completed and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                    await page.close()
                    return True
                
                # If no download yet, try to find PDF link on the page
                if not download_event:
                    logger.info(f"      → Looking for PDF link on page...")
                    pdf_url = await self.find_pdf_link_on_page(page, doc_url)
                    
                    if pdf_url and pdf_url != doc_url:
                        logger.info(f"      → Found PDF link, navigating to: {pdf_url}")
                        await page.goto(pdf_url, wait_until="commit", timeout=60000)
                        await page.wait_for_timeout(5000)
                
            except Exception as nav_error:
                # If navigation fails because download started, that's OK
                if "Download is starting" in str(nav_error):
                    logger.info(f"      → Download triggered immediately (expected behavior)")
                    # Wait for download to complete
                    await page.wait_for_timeout(10000)
                else:
                    raise nav_error
            
            # Check if download completed
            if download_completed and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                await page.close()
                return True
            
            # If download event occurred but file not saved yet, wait longer
            if download_event and not download_completed:
                logger.info(f"      → Waiting for download to complete...")
                await page.wait_for_timeout(10000)
                
                if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                    await page.close()
                    return True
            
            # Last resort: try direct content extraction
            if not os.path.exists(filepath) or os.path.getsize(filepath) < 1000:
                logger.info("      → Trying direct content extraction as fallback...")
                
                # Navigate to a fresh page to avoid download conflicts
                await page.close()
                page = await self.context.new_page()
                
                # Visit referer again
                if referer:
                    await page.goto(referer, wait_until="networkidle", timeout=60000)
                    await page.wait_for_timeout(2000)
                
                # Try to fetch PDF content directly
                pdf_content = await page.evaluate("""
                    async (url) => {
                        try {
                            const response = await fetch(url, {
                                method: 'GET',
                                credentials: 'include',
                                headers: {
                                    'Accept': 'application/pdf',
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
                            console.error('Error fetching PDF:', error);
                            return null;
                        }
                    }
                """, doc_url)
                
                if pdf_content:
                    import base64
                    with open(filepath, 'wb') as f:
                        f.write(base64.b64decode(pdf_content))
                    
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                        logger.info("      ✓ Successfully downloaded PDF using direct content extraction")
                        await page.close()
                        return True
            
            await page.close()
            
            # Final check
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                logger.info("      ✓ Successfully downloaded PDF")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"      ✗ Error downloading with Playwright: {e}")
            return False
    
    async def download_single_pdf(self, pdf_info):
        """Download a single PDF file with tracking and multiple retry strategies"""
        pdf_url = pdf_info['url']
        
        # Check if we've already tried this URL
        if pdf_url in self.downloaded_files:
            if self.downloaded_files[pdf_url]['success']:
                logger.info(f"Already successfully downloaded: {pdf_url}")
                return True
            else:
                logger.info(f"Previously failed, retrying: {pdf_url}")
        
        # Generate filename and create category subdirectory
        category = pdf_info.get('category', 'uncategorized')
        category_dir = os.path.join(self.output_dir, category)
        os.makedirs(category_dir, exist_ok=True)
        
        filename = self.generate_filename(pdf_info)
        filepath = os.path.join(category_dir, filename)
        
        # Skip if file already exists and is valid
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            logger.info(f"File already exists: {filepath}")
            self._save_tracking(pdf_url, True)
            return True
        
        # Try with multiple retries and methods
        max_retries = 3
        for retry in range(max_retries):
            try:
                logger.info(f"Downloading document, attempt {retry+1}/{max_retries} for {filename}...")
                
                # Try Playwright first
                success = await self.download_with_playwright(pdf_info, filepath)
                
                if success:
                    file_size = os.path.getsize(filepath)
                    logger.info(f"      ✓ Successfully downloaded {filename} ({file_size / 1024:.2f} KB)")
                    self._save_tracking(pdf_url, True)
                    return True
                
                # If Playwright fails, try curl as fallback on last attempt
                if retry == max_retries - 1:
                    logger.info("      → Playwright failed, trying curl as final attempt...")
                    success = await self.download_with_curl(pdf_url, filepath, pdf_info.get('referer'))
                    if success:
                        file_size = os.path.getsize(filepath)
                        logger.info(f"      ✓ Successfully downloaded with curl {filename} ({file_size / 1024:.2f} KB)")
                        self._save_tracking(pdf_url, True)
                        return True
                
                logger.warning(f"      ⚠ Download failed for {filename}, attempt {retry+1}")
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"      ✗ Error downloading {filename}: {e}")
                await asyncio.sleep(3)
        
        # If all retries fail
        logger.error(f"All download attempts failed for {filename}")
        self._save_tracking(pdf_url, False)
        return False
    
    async def process_va_publications(self, selected_categories=None):
        """Process VA publications from selected categories in headless mode"""
        try:
            await self.initialize_browser()
            
            # Determine which categories to process
            if selected_categories:
                categories_to_process = {k: v for k, v in self.category_urls.items() if v in selected_categories}
            else:
                categories_to_process = self.category_urls
            
            total_categories = len(categories_to_process)
            logger.info(f"Processing {total_categories} VA categories...")
            
            # Process each category
            for cat_idx, (category_url, category_name) in enumerate(categories_to_process.items(), 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing Category {cat_idx}/{total_categories}: {category_name}")
                logger.info(f"{'='*60}")
                
                # Extract document links from the current category
                pdf_links = await self.extract_pdf_links(category_url, category_name)
                
                if not pdf_links:
                    logger.warning(f"No document links found in category: {category_name}")
                    continue
                
                logger.info(f"Found {len(pdf_links)} documents in {category_name}")
                
                # Download PDFs from this category immediately
                for pdf_idx, pdf_info in enumerate(pdf_links, 1):
                    logger.info(f"\n[{pdf_idx}/{len(pdf_links)}] Processing: {pdf_info['name'][:60]}...")
                    await self.download_single_pdf(pdf_info)
                    # Add a small delay between downloads
                    await asyncio.sleep(2)
                
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
    """Main function to run the VA scraper in headless mode"""
    scraper = VAPublicationScraper()
    
    print("\n" + "="*60)
    print("VA Publications PDF Scraper (Playwright - Headless Mode)")
    print("="*60)
    print("This script will download publications from the Department of Veterans Affairs.")
    print("It will process categories sequentially and download PDFs automatically.")
    print("Progress is tracked, so you can resume if interrupted.")
    print("="*60 + "\n")
    
    # Display available categories
    print("Available VA Categories:")
    for idx, (url, name) in enumerate(scraper.category_urls.items(), 1):
        print(f"  {idx}. {name}")
    
    print("\n" + "="*60)
    category_choice = input("\nProcess all categories or select specific ones? (all/select): ").strip().lower()
    
    selected_categories = None
    if category_choice == 'select':
        print("\nEnter category numbers separated by commas (e.g., 1,3,5):")
        selections = input("Categories: ").strip()
        try:
            indices = [int(x.strip()) for x in selections.split(',')]
            category_names = list(scraper.category_urls.values())
            selected_categories = [category_names[i-1] for i in indices if 0 < i <= len(category_names)]
            print(f"\nSelected categories: {', '.join(selected_categories)}")
        except:
            print("Invalid selection. Processing all categories.")
            selected_categories = None
    
    print("\n" + "="*60)
    print("Starting VA Publications scraper...")
    print("Press Ctrl+C at any time to stop the process. Progress will be saved.")
    print("="*60 + "\n")
    
    await scraper.process_va_publications(selected_categories)
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETED")
    print("="*60)
    print(f"Downloaded files are saved in: {os.path.abspath(scraper.output_dir)}")
    print(f"Check the log file for details: va_scraper.log")
    print("="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(main())