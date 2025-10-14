import os
import time
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import logging
import json
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse
import subprocess
import base64

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("army_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class ArmyPublicationScraper:
    def __init__(self, base_url="https://armypubs.army.mil/", output_dir="army_pdfs"):
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
        
        # List of category URLs to process
        self.category_urls = [
            # Removed 'https://armypubs.army.mil/ProductMaps/PubForm/Web_Series.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/ALARACT.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/ArmyDir.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/AGO.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/DAMEMO.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/AR.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/HQDAPolicyNotice.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/PAM.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/PogProponent.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/PPM.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/EM.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/ADP.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/ATP.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/CTA.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/FM.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/JTA.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/TC.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/TM_Admin.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/TB_Cal.aspx',
            'https://armypubs.army.mil/ProductMaps/PubForm/StrategicDocuments.aspx'
        ]
        
        # List of problematic URLs to skip
        self.problematic_urls = [
            "hqda-form-11-20250416.pdf",
            "HQDAFORM43_Blank.pdf",
            "api.army.mil"
        ]
        
    def _load_tracking(self):
        """Load tracking information from file"""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_tracking(self, pdf_url, success, pub_number="", title=""):
        """Save tracking information to file"""
        self.downloaded_files[pdf_url] = {
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'pub_number': pub_number,
            'title': title
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
                ignore_default_args=["--enable-automation"],
                args=["--ignore-certificate-errors", "--allow-insecure-localhost"]  # Ignore certificate errors
            )
            
            # Create a persistent context with custom settings
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True,
                ignore_https_errors=True  # Ignore HTTPS errors
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
    
    async def extract_publication_links(self, category_url):
        """Extract links to individual publication pages from a category page"""
        publication_links = []
        publication_info = []
        
        content = await self.get_page_content(category_url)
        
        if not content:
            logger.error(f"Failed to get content from {category_url}")
            return publication_links, publication_info
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Find the main table that contains the publications
        tables = soup.select('table')
        
        for table in tables:
            rows = table.select('tr')
            
            # Skip header row
            for row in rows[1:] if len(rows) > 1 else []:
                cells = row.select('td')
                
                if len(cells) >= 3:  # Ensure we have enough cells
                    # Look for links in the first cell (typically the number/ID column)
                    link_cell = cells[0]
                    links = link_cell.select('a')
                    
                    if links:
                        href = links[0].get('href')
                        if href:
                            # Extract publication info
                            pub_number = links[0].get_text(strip=True)
                            
                            # Get title from the appropriate cell (usually the 3rd or 4th)
                            title = ""
                            if len(cells) > 3:
                                title = cells[3].get_text(strip=True)
                            elif len(cells) > 2:
                                title = cells[2].get_text(strip=True)
                            
                            # Get status if available
                            status = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                            
                            full_url = urljoin(category_url, href)
                            publication_links.append(full_url)
                            publication_info.append({
                                'url': full_url,
                                'number': pub_number,
                                'title': title,
                                'status': status
                            })
                            
                            logger.info(f"Found publication: {pub_number} - {title}")
        
        return publication_links, publication_info
    
    async def extract_pdf_link(self, publication_url, pub_info):
        """Extract PDF download link from a publication detail page"""
        content = await self.get_page_content(publication_url)
        
        if not content:
            logger.error(f"Failed to get content from {publication_url}")
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look specifically for the PDF link in the Unit Of Issue(s) row
        pdf_cell = None
        for row in soup.select('tr'):
            cells = row.select('td')
            if len(cells) >= 1 and 'Unit Of Issue' in cells[0].get_text():
                if len(cells) > 1:
                    pdf_cell = cells[1]
                    break
        
        # If we found the PDF cell, extract the link
        if pdf_cell:
            pdf_link = pdf_cell.select_one('a')
            if pdf_link and pdf_link.get_text(strip=True) == 'PDF':
                href = pdf_link.get('href')
                if href:
                    full_url = urljoin(publication_url, href)
                    
                    # Skip problematic URLs
                    if any(prob_url in full_url for prob_url in self.problematic_urls):
                        logger.warning(f"Skipping problematic URL: {full_url}")
                        return None
                    
                    logger.info(f"Found PDF link: {full_url}")
                    return full_url
        
        # If we didn't find the specific PDF link, look for any PDF link
        for link in soup.select('a'):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if (text == 'PDF' or href.lower().endswith('.pdf')) and not any(prob_url in href for prob_url in self.problematic_urls):
                full_url = urljoin(publication_url, href)
                logger.info(f"Found alternative PDF link: {full_url}")
                return full_url
        
        logger.warning(f"No PDF link found in {publication_url}")
        return None
    
    def generate_filename(self, pdf_url, pub_info):
        """Generate a unique filename based on the PDF URL and publication info"""
        # Use publication number as part of filename if available
        pub_number = pub_info.get('number', '')
        if pub_number:
            # Clean up publication number for filename
            pub_number = re.sub(r'[^\w\-\.]', '_', pub_number)
            
        # Extract filename from PDF URL
        url_filename = os.path.basename(urlparse(pdf_url).path).split('?')[0]
        
        # Clean up filename
        url_filename = re.sub(r'[^\w\-\.]', '_', url_filename)
        if not url_filename.lower().endswith('.pdf'):
            url_filename += '.pdf'
        
        # Create a unique filename combining publication info and PDF name
        if pub_number:
            return f"{pub_number}_{url_filename}"
        else:
            return url_filename
    
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
                try:
                    download_event = download
                    await download.save_as(download_path)
                    logger.info(f"Download completed and saved to {download_path}")
                except Exception as e:
                    logger.error(f"Error saving download: {e}")
            
            page.on("download", handle_download)
            
            # Now navigate to the PDF URL
            logger.info(f"Navigating to PDF URL: {pdf_url}")
            
            try:
                # Use a different approach for PDF files
                if pdf_url.lower().endswith('.pdf'):
                    # For direct PDF links, use fetch API instead of navigation
                    pdf_content = await page.evaluate("""
                        async (url) => {
                            try {
                                const response = await fetch(url, {
                                    method: 'GET',
                                    credentials: 'include',
                                    headers: {
                                        'Accept': 'application/pdf',
                                        'Referer': document.location.href
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
                            logger.info(f"Successfully downloaded PDF using fetch API: {filepath}")
                            await page.close()
                            return True
                    else:
                        # If fetch API fails, try direct navigation
                        await page.goto(pdf_url, wait_until="domcontentloaded", timeout=60000)
                else:
                    # For non-PDF URLs, use normal navigation
                    await page.goto(pdf_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                # Check if this is a download error, which might actually be successful
                if "Download is starting" in str(e):
                    logger.info("Download started automatically")
                    # Wait for the download to complete
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Error navigating to PDF: {e}")
            
            # Wait for download to start or timeout after 30 seconds
            start_time = time.time()
            while download_event is None and (time.time() - start_time) < 30:
                await page.wait_for_timeout(1000)
            
            # Close the page
            await page.close()
            
            # Check if the file was downloaded and is valid
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                logger.info(f"Successfully downloaded PDF: {filepath}")
                return True
            else:
                # Try one more direct approach - use curl via subprocess
                try:
                    logger.info(f"Trying curl download for: {pdf_url}")
                    subprocess.run([
                        "curl", "-L", "-k", "-o", filepath,  # Added -k to ignore certificate errors
                        "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                        "-e", publication_url,
                        pdf_url
                    ], check=True, timeout=60)
                    
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                        logger.info(f"Successfully downloaded PDF using curl: {filepath}")
                        return True
                except Exception as e:
                    logger.error(f"Curl download failed: {e}")
                    
            return False
        except Exception as e:
            logger.error(f"Error downloading with Playwright: {e}")
            return False
    
    async def download_single_pdf(self, pdf_url, publication_url, pub_info):
        """Download a single PDF file with tracking and retries"""
        # Skip problematic URLs
        if any(prob_url in pdf_url for prob_url in self.problematic_urls):
            logger.warning(f"Skipping problematic URL: {pdf_url}")
            return False
            
        # Check if we've already tried this URL
        if pdf_url in self.downloaded_files:
            if self.downloaded_files[pdf_url]['success']:
                logger.info(f"Already successfully downloaded: {pdf_url}")
                return True
            else:
                logger.info(f"Previously failed, retrying: {pdf_url}")
        
        # Generate a unique filename for the PDF
        filename = self.generate_filename(pdf_url, pub_info)
        filepath = os.path.join(self.output_dir, filename)
        
        # Skip if file already exists and is valid
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            logger.info(f"File already exists: {filepath}")
            self._save_tracking(pdf_url, True, pub_info.get('number', ''), pub_info.get('title', ''))
            return True
        
        # Try with multiple retries
        max_retries = 3
        for retry in range(max_retries):
            try:
                logger.info(f"Downloading PDF, attempt {retry+1}/{max_retries} for {filename}...")
                success = await self.download_with_playwright(pdf_url, filepath, publication_url)
                if success:
                    logger.info(f"Successfully downloaded {filename}")
                    self._save_tracking(pdf_url, True, pub_info.get('number', ''), pub_info.get('title', ''))
                    return True
                else:
                    logger.warning(f"Download failed for {filename}, attempt {retry+1}")
                    await asyncio.sleep(3)  # Wait before retry
            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")
                await asyncio.sleep(3)  # Wait before retry
        
        # If all retries fail
        logger.error(f"All download attempts failed for {filename}")
        self._save_tracking(pdf_url, False, pub_info.get('number', ''), pub_info.get('title', ''))
        return False
    
    async def process_army_publications(self):
        """Process Army publications from all category pages"""
        try:
            await self.initialize_browser()
            
            for category_url in self.category_urls:
                logger.info(f"Processing category: {category_url}")
                
                # Get publication links from the category page
                publication_links, publication_info = await self.extract_publication_links(category_url)
                
                if not publication_links:
                    logger.warning(f"No publication links found in {category_url}")
                    continue
                
                # Process each publication immediately
                for i, (pub_link, pub_info) in enumerate(zip(publication_links, publication_info)):
                    logger.info(f"Processing publication {i+1}/{len(publication_links)}: {pub_info.get('number', '')} - {pub_info.get('title', '')}")
                    
                    # Extract PDF link from the publication page
                    pdf_link = await self.extract_pdf_link(pub_link, pub_info)
                    
                    # Skip problematic URLs
                    if pdf_link and any(prob_url in pdf_link for prob_url in self.problematic_urls):
                        logger.warning(f"Skipping problematic PDF link: {pdf_link}")
                        continue
                    
                    # Download PDF if link was found
                    if pdf_link:
                        await self.download_single_pdf(pdf_link, pub_link, pub_info)
                        # Add a small delay between downloads
                        await asyncio.sleep(2)
                    else:
                        logger.warning(f"No PDF link found for {pub_info.get('number', '')} - {pub_info.get('title', '')}")
                
                # Add a delay between categories
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Process interrupted by user. Progress saved, you can resume later.")
        except Exception as e:
            logger.error(f"Error in processing: {e}")
        finally:
            await self.close_browser()
            logger.info("Processing completed.")

async def main():
    """Main function to run the scraper in headless mode"""
    scraper = ArmyPublicationScraper()
    
    print("Army Publication Scraper (Headless Mode)")
    print("---------------------------------------")
    print("This script will download publications from the Army Publications website.")
    print("It will process all category pages and download PDFs automatically.")
    print("Progress is tracked, so you can resume if interrupted.")
    
    print("\nStarting scraper...")
    print("Press Ctrl+C at any time to stop the process. Progress will be saved.\n")
    
    await scraper.process_army_publications()

if __name__ == "__main__":
    asyncio.run(main())
                            