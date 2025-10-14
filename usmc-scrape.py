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
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("usmc_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class MarineCorpsPublicationScraper:
    def __init__(self, base_url="https://www.marines.mil/News/Publications/MCPEL/", output_dir="marine_pdfs"):
        self.base_url = base_url
        self.output_dir = output_dir
        
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True
            )
            
            # Add custom headers that will be sent with every request
            await self.context.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0"
            })
            
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
            await page.wait_for_timeout(2000)
            
            # Get the page content
            content = await page.content()
            
            await page.close()
            return content
        except Exception as e:
            logger.error(f"Error getting page content for {url}: {e}")
            return None
    
    async def extract_publication_links(self, page_url):
        """Extract links to individual publication pages from a listing page"""
        publication_links = []
        content = await self.get_page_content(page_url)
        
        if not content:
            logger.error(f"Failed to get content from {page_url}")
            return publication_links
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Try multiple selectors to find publication links
        selectors = [
            'a.publication-link',
            'a[href*="/Publications/MCPEL/Electronic-Library-Display/Article/"]',
            '.item a',  # Generic item links
            'a[href*="Article"]'  # Any link with Article in the URL
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            if links:
                break
        
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                publication_links.append(full_url)
                logger.info(f"Found publication link: {full_url}")
        
        return publication_links
    
    async def extract_pdf_links(self, publication_url):
        """Extract PDF download links from a publication page"""
        pdf_links = []
        content = await self.get_page_content(publication_url)
        
        if not content:
            logger.error(f"Failed to get content from {publication_url}")
            return pdf_links
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Try multiple selectors to find PDF links
        selectors = [
            'a[href$=".pdf"]',
            'a.download-link',
            'a:contains("DOWNLOAD PDF")',
            'a[href*="pdf"]',
            'a[href*="Portals"]'  # Many PDFs are in the Portals directory
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                # BeautifulSoup doesn't support :contains, so we need a custom approach
                if 'DOWNLOAD PDF' in selector:
                    links = [a for a in soup.find_all('a') if 'DOWNLOAD PDF' in a.get_text()]
            else:
                links = soup.select(selector)
            
            if links:
                for link in links:
                    href = link.get('href')
                    if href and (href.endswith('.pdf') or 'pdf' in href.lower() or 'Portals' in href):
                        full_url = urljoin(publication_url, href)
                        pdf_links.append(full_url)
                        logger.info(f"Found PDF link: {full_url}")
        
        return pdf_links
    
    def generate_filename(self, pdf_url, publication_url):
        """Generate a unique filename based on the PDF URL and publication URL"""
        # Extract publication info from URL
        pub_match = re.search(r'/Article/(\d+)/([^/]+)', publication_url)
        pub_id = pub_match.group(1) if pub_match else "unknown"
        pub_name = pub_match.group(2) if pub_match else "unknown"
        
        # Extract filename from PDF URL
        filename = os.path.basename(pdf_url).split('?')[0]  # Remove query parameters
        
        # Clean up filename
        filename = re.sub(r'[^\w\-\.]', '_', filename)
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        # Create a unique filename combining publication info and PDF name
        return f"{pub_id}_{filename}"
    
    async def download_with_playwright(self, pdf_url, filepath, publication_url):
        """Download PDF using Playwright with full browser simulation"""
        if not self.browser:
            await self.initialize_browser()
            
        try:
            # Create a new page
            page = await self.context.new_page()
            
            # First visit the main site to establish cookies and session
            await page.goto(self.base_url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)
            
            # Then visit the publication page that contains the PDF link
            await page.goto(publication_url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)
            
            # Set up download listener
            download_path = filepath
            download_event = None
            
            async def handle_download(download):
                nonlocal download_event
                download_event = download
                await download.save_as(download_path)
            
            page.on("download", handle_download)
            
            # Now navigate to the PDF URL
            logger.info(f"Navigating to PDF URL: {pdf_url}")
            await page.goto(pdf_url, wait_until="domcontentloaded", timeout=60000)
            
            # Wait for download to start or timeout after 30 seconds
            start_time = time.time()
            while download_event is None:
                await page.wait_for_timeout(1000)
                if time.time() - start_time > 30:
                    break
            
            # If download didn't start automatically, try to get the content directly
            if download_event is None:
                logger.info("Download didn't start automatically, trying direct content extraction")
                
                # Try to get the PDF content directly using fetch API
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
                """, pdf_url)
                
                if pdf_content:
                    import base64
                    with open(filepath, 'wb') as f:
                        f.write(base64.b64decode(pdf_content))
                    
                    # Check if the file is valid
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                        logger.info("Successfully downloaded PDF using direct content extraction")
                        await page.close()
                        return True
            
            # Close the page
            await page.close()
            
            # Check if the file was downloaded and is valid
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                logger.info("Successfully downloaded PDF")
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error downloading with Playwright: {e}")
            return False
    
    async def download_single_pdf(self, pdf_url, publication_url):
        """Download a single PDF file with tracking and retries"""
        # Check if we've already tried this URL
        if pdf_url in self.downloaded_files:
            if self.downloaded_files[pdf_url]['success']:
                logger.info(f"Already successfully downloaded: {pdf_url}")
                return True
            else:
                logger.info(f"Previously failed, retrying: {pdf_url}")
        
        # Generate a unique filename for the PDF
        filename = self.generate_filename(pdf_url, publication_url)
        filepath = os.path.join(self.output_dir, filename)
        
        # Skip if file already exists and is valid
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            logger.info(f"File already exists: {filepath}")
            self._save_tracking(pdf_url, True)
            return True
        
        # Try with multiple retries
        max_retries = 3
        for retry in range(max_retries):
            try:
                logger.info(f"Downloading PDF, attempt {retry+1}/{max_retries} for {filename}...")
                success = await self.download_with_playwright(pdf_url, filepath, publication_url)
                if success:
                    logger.info(f"Successfully downloaded {filename}")
                    self._save_tracking(pdf_url, True)
                    return True
                else:
                    logger.warning(f"Download failed for {filename}, attempt {retry+1}")
                    time.sleep(3)  # Wait before retry
            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")
                time.sleep(3)  # Wait before retry
        
        # If all retries fail
        logger.error(f"All download attempts failed for {filename}")
        self._save_tracking(pdf_url, False)
        return False
    
    async def process_marine_publications(self, start_page=1, max_pages=None):
        """Process Marine Corps publications page by page in headless mode"""
        try:
            await self.initialize_browser()
            current_page = start_page
            
            while True:
                logger.info(f"Processing page {current_page}...")
                
                # Get publication links from the current page
                page_url = f"{self.base_url}?Page={current_page}"
                publication_links = await self.extract_publication_links(page_url)
                
                if not publication_links:
                    logger.warning(f"No publication links found on page {current_page}")
                    if current_page > start_page:  # Only break if we've processed at least one page
                        logger.info("No more publications found. Stopping.")
                        break
                
                # Process each publication immediately
                for pub_link in publication_links:
                    logger.info(f"Processing publication: {pub_link}")
                    pdf_links = await self.extract_pdf_links(pub_link)
                    
                    # Download PDFs from this publication immediately
                    if pdf_links:
                        for pdf_link in pdf_links:
                            await self.download_single_pdf(pdf_link, pub_link)
                            # Add a small delay between downloads
                            await asyncio.sleep(2)
                    else:
                        logger.warning(f"No PDF links found in {pub_link}")
                
                # Check if we've reached the maximum number of pages
                if max_pages and current_page >= start_page + max_pages - 1:
                    logger.info(f"Reached maximum number of pages ({max_pages}). Stopping.")
                    break
                
                current_page += 1
                # Add a delay between pages
                await asyncio.sleep(3)
                
        except KeyboardInterrupt:
            logger.info("Process interrupted by user. Progress saved, you can resume later.")
        except Exception as e:
            logger.error(f"Error in processing: {e}")
        finally:
            await self.close_browser()
            logger.info("Processing completed.")

async def main():
    """Main function to run the scraper in headless mode"""
    scraper = MarineCorpsPublicationScraper()
    
    print("Marine Corps Publication Scraper (Headless Mode)")
    print("-----------------------------------------------")
    print("This script will download publications from the Marine Corps Publications Electronic Library.")
    print("It will process pages sequentially and download PDFs automatically.")
    print("Progress is tracked, so you can resume if interrupted.")
    
    start_page = int(input("Enter starting page number (default: 1): ") or "1")
    max_pages = input("Enter maximum number of pages to process (leave empty for all): ")
    max_pages = int(max_pages) if max_pages else None
    
    print(f"\nStarting scraper from page {start_page}" + 
          (f", processing up to {max_pages} pages" if max_pages else ""))
    print("Press Ctrl+C at any time to stop the process. Progress will be saved.\n")
    
    await scraper.process_marine_publications(start_page, max_pages)

if __name__ == "__main__":
    asyncio.run(main())