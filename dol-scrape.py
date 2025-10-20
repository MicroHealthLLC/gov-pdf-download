import asyncio
import re
from pathlib import Path
from playwright.async_api import async_playwright
import aiohttp
from urllib.parse import urljoin, urlparse, unquote
import os
from typing import Set, List, Dict
import hashlib

class DOLDocumentScraper:
    def __init__(self, output_dir="downloaded_docs", max_depth=2):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.visited_urls: Set[str] = set()
        self.downloaded_files: Set[str] = set()
        self.document_extensions = ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx', 
                                     '.ppt', '.pptx', '.txt', '.zip']
        self.max_depth = max_depth
        self.base_domain = "dol.gov"
        self.stats = {
            'pages_visited': 0,
            'documents_downloaded': 0,
            'advisory_pages_processed': 0,
            'listing_pages_processed': 0
        }
        
    def is_document_url(self, url: str) -> bool:
        """Check if URL points to a document"""
        parsed = urlparse(url.lower())
        return any(parsed.path.endswith(ext) for ext in self.document_extensions)
    
    def is_valid_dol_url(self, url: str) -> bool:
        """Check if URL is within DOL domain"""
        parsed = urlparse(url)
        return self.base_domain in parsed.netloc.lower()
    
    def generate_filename(self, url: str, context: str = "") -> str:
        """Generate a unique filename from URL with context"""
        parsed = urlparse(url)
        filename = os.path.basename(unquote(parsed.path))
        
        if not filename or filename in ['index.html', 'index.htm', '']:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            ext = self.get_extension(url)
            filename = f"document_{url_hash}{ext}"
        
        if context:
            safe_context = re.sub(r'[^\w\-_]', '_', context)[:50]
            name, ext = os.path.splitext(filename)
            filename = f"{safe_context}_{name}{ext}"
        
        return filename
    
    def get_extension(self, url: str) -> str:
        """Extract file extension from URL"""
        parsed = urlparse(url.lower())
        for ext in self.document_extensions:
            if parsed.path.endswith(ext):
                return ext
        return '.pdf'
    
    async def download_document(self, session: aiohttp.ClientSession, url: str, context: str = "") -> bool:
        """Download document using aiohttp"""
        filename = self.generate_filename(url, context)
        
        if filename in self.downloaded_files:
            print(f"  ⊘ Already downloaded: {filename}")
            return False
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,application/vnd.ms-excel,application/msword,*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.dol.gov/guidance'
            }
            
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    content = await response.read()
                    filepath = self.output_dir / filename
                    
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    
                    self.downloaded_files.add(filename)
                    self.stats['documents_downloaded'] += 1
                    print(f"  ✓ Downloaded: {filename} ({len(content):,} bytes)")
                    return True
                else:
                    print(f"  ✗ Failed: {filename} (Status {response.status})")
                    return False
        except Exception as e:
            print(f"  ✗ Error downloading {filename}: {str(e)}")
            return False
    
    async def extract_document_links_from_advisory_page(self, page) -> List[Dict[str, str]]:
        """Extract document links from an advisory/guidance detail page"""
        document_links = []
        
        try:
            # Extract from DOCUMENTS section
            docs_section = await page.query_selector('#etadocs, .documents.section')
            if docs_section:
                links = await docs_section.query_selector_all('a[href]')
                for link in links:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    if href and self.is_document_url(href):
                        absolute_url = urljoin(page.url, href)
                        document_links.append({
                            'url': absolute_url,
                            'text': text.strip(),
                            'section': 'DOCUMENTS'
                        })
            
            # Extract from ATTACHMENTS section
            attachments_section = await page.query_selector('#etaattachments, .attachments.section')
            if attachments_section:
                links = await attachments_section.query_selector_all('a[href]')
                for link in links:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    if href and self.is_document_url(href):
                        absolute_url = urljoin(page.url, href)
                        document_links.append({
                            'url': absolute_url,
                            'text': text.strip(),
                            'section': 'ATTACHMENTS'
                        })
            
            # Check for other document links
            content_div = await page.query_selector('#advisory-detail, .advisory-content')
            if content_div:
                all_links = await content_div.query_selector_all('a[href]')
                for link in all_links:
                    href = await link.get_attribute('href')
                    if href and self.is_document_url(href):
                        absolute_url = urljoin(page.url, href)
                        if not any(d['url'] == absolute_url for d in document_links):
                            text = await link.inner_text()
                            document_links.append({
                                'url': absolute_url,
                                'text': text.strip(),
                                'section': 'OTHER'
                            })
        
        except Exception as e:
            print(f"  ✗ Error extracting document links: {str(e)}")
        
        return document_links
    
    async def process_advisory_page(self, page, session: aiohttp.ClientSession, url: str) -> int:
        """Process a single advisory/guidance page and download all documents"""
        if url in self.visited_urls:
            return 0
        
        self.visited_urls.add(url)
        self.stats['pages_visited'] += 1
        
        try:
            print(f"\n  → Processing advisory page: {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=15000)
            await asyncio.sleep(2)
            
            # Extract the advisory title for context
            title_elem = await page.query_selector('h1, .title h1')
            advisory_title = "unknown"
            if title_elem:
                advisory_title = await title_elem.inner_text()
                advisory_title = re.sub(r'[^\w\-_]', '_', advisory_title.strip())[:30]
            
            # Extract document links from specific sections
            document_links = await self.extract_document_links_from_advisory_page(page)
            
            if document_links:
                print(f"  → Found {len(document_links)} document(s) on this page")
                self.stats['advisory_pages_processed'] += 1
                
                # Download each document
                for doc in document_links:
                    print(f"    [{doc['section']}] {doc['text'][:60]}")
                    await self.download_document(session, doc['url'], advisory_title)
                    await asyncio.sleep(1.5)
                
                return len(document_links)
            else:
                print(f"  ⚠ No documents found on this page")
                return 0
        
        except Exception as e:
            print(f"  ✗ Error processing advisory page: {str(e)}")
            return 0
    
    async def extract_advisory_links(self, page) -> List[str]:
        """Extract links to advisory/guidance pages from listing page"""
        advisory_links = []
        
        try:
            # Look for links in the guidance results wrapper
            result_wrappers = await page.query_selector_all('.eo-guidance-view-results-wrapper')
            
            for wrapper in result_wrappers:
                # Find the title link within each result
                title_link = await wrapper.query_selector('.eo-guidance-view-result.title a')
                if title_link:
                    href = await title_link.get_attribute('href')
                    if href:
                        absolute_url = urljoin(page.url, href)
                        
                        # Only add if it's a DOL URL and not a direct document
                        if (self.is_valid_dol_url(absolute_url) and 
                            not self.is_document_url(absolute_url) and
                            absolute_url not in advisory_links):
                            advisory_links.append(absolute_url)
        
        except Exception as e:
            print(f"Error extracting advisory links: {str(e)}")
        
        return advisory_links
    
    async def run(self, base_url: str, start_page: int = 0, end_page: int = 951):
        """
        Main scraping function with programmatic pagination
        base_url: Base URL (e.g., "https://www.dol.gov/guidance")
        start_page: Starting page number (default: 0)
        end_page: Ending page number (default: 951)
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                accept_downloads=True
            )
            
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            """)
            
            page = await context.new_page()
            
            async with aiohttp.ClientSession() as session:
                print(f"{'='*70}")
                print(f"DOL Document Scraper Started")
                print(f"{'='*70}")
                print(f"Base URL: {base_url}")
                print(f"Pages to scrape: {start_page} to {end_page} ({end_page - start_page + 1} pages)")
                print(f"Output: {self.output_dir.absolute()}\n")
                
                # Loop through each page programmatically
                for page_num in range(start_page, end_page + 1):
                    # Construct the URL for this page
                    current_url = f"{base_url}?page={page_num}"
                    
                    print(f"\n{'='*70}")
                    print(f"PROCESSING LISTING PAGE {page_num + 1} of {end_page + 1}")
                    print(f"URL: {current_url}")
                    print(f"{'='*70}")
                    
                    try:
                        # Navigate to the page
                        await page.goto(current_url, wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(2.5)
                        
                        self.stats['listing_pages_processed'] += 1
                        
                        # Extract advisory page links from current listing page
                        advisory_links = await self.extract_advisory_links(page)
                        print(f"Found {len(advisory_links)} advisory pages to process")
                        
                        # Process each advisory page
                        for idx, advisory_url in enumerate(advisory_links, 1):
                            print(f"\n[{idx}/{len(advisory_links)}] Processing advisory...")
                            await self.process_advisory_page(page, session, advisory_url)
                            await asyncio.sleep(2)  # Delay between advisory pages
                        
                        # Progress update
                        progress = ((page_num - start_page + 1) / (end_page - start_page + 1)) * 100
                        print(f"\n→ Progress: {progress:.1f}% complete ({page_num - start_page + 1}/{end_page - start_page + 1} pages)")
                        
                    except Exception as e:
                        print(f"✗ Error processing page {page_num}: {str(e)}")
                        continue
                
                # Print final statistics
                print(f"\n{'='*70}")
                print(f"SCRAPING COMPLETE")
                print(f"{'='*70}")
                print(f"Listing pages processed: {self.stats['listing_pages_processed']}")
                print(f"Total pages visited: {self.stats['pages_visited']}")
                print(f"Advisory pages with documents: {self.stats['advisory_pages_processed']}")
                print(f"Documents downloaded: {self.stats['documents_downloaded']}")
                print(f"Unique files: {len(self.downloaded_files)}")
                print(f"Output directory: {self.output_dir.absolute()}")
            
            await browser.close()


# Usage Examples
async def main():
    """Main function with different usage scenarios"""
    
    # Scenario 1: Process ALL 952 pages (0-951) - FULL SCRAPE
    scraper = DOLDocumentScraper(output_dir="dol_documents_full", max_depth=2)
    await scraper.run("https://www.dol.gov/guidance", start_page=0, end_page=951)
    
    # Scenario 2: Process first 10 pages only (for testing)
    # scraper = DOLDocumentScraper(output_dir="dol_documents_test", max_depth=2)
    # await scraper.run("https://www.dol.gov/guidance", start_page=0, end_page=9)
    
    # Scenario 3: Resume from page 100 to 200
    # scraper = DOLDocumentScraper(output_dir="dol_documents_partial", max_depth=2)
    # await scraper.run("https://www.dol.gov/guidance", start_page=100, end_page=200)


if __name__ == "__main__":
    asyncio.run(main())