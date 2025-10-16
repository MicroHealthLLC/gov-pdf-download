import asyncio
import re
from pathlib import Path
from playwright.async_api import async_playwright
import aiohttp

class VTechWorksBookScraper:
    def __init__(self, download_dir="vtechworks_books"):
        self.base_url = "https://vtechworks.lib.vt.edu"
        self.collection_url = "https://vtechworks.lib.vt.edu/collections/a2d17483-d5d5-4733-9326-d64dd88f258d"
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        
    async def get_books_from_page(self, page, page_num):
        """Extract all book links from a single page"""
        try:
            url = f"{self.collection_url}?cp.page={page_num}"
            print(f"  Scraping page {page_num}...")
            
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)
            
            book_links = set()
            all_links = await page.locator('a').all()
            
            for link in all_links:
                try:
                    href = await link.get_attribute('href')
                    if href and '/items/' in href:
                        if href.startswith('/'):
                            full_url = f"{self.base_url}{href}"
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue
                        
                        full_url = full_url.split('?')[0].split('#')[0]
                        
                        if '/items/' in full_url and '/collections/' not in full_url:
                            book_links.add(full_url)
                            
                except Exception as e:
                    continue
            
            book_list = sorted(list(book_links))
            print(f"  Found {len(book_list)} books on page {page_num}")
            
            return book_list
            
        except Exception as e:
            print(f"  ERROR on page {page_num}: {e}")
            return []
    
    async def get_all_book_links(self, page):
        """Extract all book links from pages 1-3"""
        all_books = []
        
        for page_num in range(1, 4):
            books = await self.get_books_from_page(page, page_num)
            all_books.extend(books)
            await asyncio.sleep(2)
        
        unique_books = list(dict.fromkeys(all_books))
        return unique_books
    
    async def extract_pdf_download_link(self, page, book_url):
        """Extract the bitstream download link from book page"""
        try:
            await page.goto(book_url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)
            
            title_locator = page.locator('h1, h2, .page-title').first
            if await title_locator.count() > 0:
                title_text = await title_locator.text_content()
                title = re.sub(r'[^\w\s-]', '', title_text).strip()
                title = re.sub(r'[-\s]+', '_', title)[:100]
            else:
                title = book_url.split('/')[-1]
            
            download_links = await page.locator('a[href*="/bitstreams/"]').all()
            
            for link in download_links:
                try:
                    href = await link.get_attribute('href')
                    parent = link.locator('xpath=..')
                    parent_text = await parent.text_content() if await parent.count() > 0 else ""
                    link_text = await link.text_content()
                    combined_text = f"{link_text} {parent_text}".lower()
                    
                    if any(skip in combined_text for skip in ['low resolution', 'low res', 'epub', '.zip', '.epub']):
                        continue
                    
                    if '.pdf' in combined_text and '/download' in href:
                        size_match = re.search(r'\(([^)]*(?:mb|kb|gb)[^)]*)\)', combined_text, re.IGNORECASE)
                        file_size = size_match.group(1) if size_match else "unknown"
                        
                        download_url = f"{self.base_url}{href}" if href.startswith('/') else href
                        
                        return {
                            'url': download_url,
                            'title': title,
                            'size': file_size,
                            'book_url': book_url
                        }
                        
                except Exception as e:
                    continue
            
            return None
                    
        except Exception as e:
            print(f"  ERROR extracting from {book_url}: {e}")
            return None
    
    async def download_file(self, session, download_info, index, total):
        """Download a single PDF file"""
        try:
            filename = f"{download_info['title']}.pdf"
            filepath = self.download_dir / filename
            
            if filepath.exists():
                print(f"  [{index}/{total}] ✓ Already exists: {filename}")
                return True
            
            print(f"  [{index}/{total}] ⬇ Downloading: {filename} ({download_info['size']})")
            
            async with session.get(download_info['url'], timeout=aiohttp.ClientTimeout(total=600)) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    file_size_mb = len(content) / (1024 * 1024)
                    print(f"  [{index}/{total}] ✓ Completed: {filename} ({file_size_mb:.2f} MB)")
                    return True
                else:
                    print(f"  [{index}/{total}] ✗ Failed: {filename} (HTTP {response.status})")
                    return False
                    
        except Exception as e:
            print(f"  [{index}/{total}] ✗ Error: {download_info['title']} - {e}")
            return False
    
    async def run(self):
        """Main execution method"""
        async with async_playwright() as p:
            print("Launching browser in headless mode...")
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            print("=" * 70)
            print("VTechWorks Open Textbooks Scraper (Production Version)")
            print("=" * 70)
            
            print("\n[1/3] Collecting book links from all pages...")
            book_links = await self.get_all_book_links(page)
            print(f"\n✓ Found {len(book_links)} total unique books")
            
            if len(book_links) == 0:
                print("\n⚠ ERROR: No books found!")
                await browser.close()
                return
            
            print(f"\n[2/3] Extracting PDF download links from {len(book_links)} books...")
            download_list = []
            
            for i, book_url in enumerate(book_links, 1):
                print(f"  Processing book {i}/{len(book_links)}...")
                pdf_info = await self.extract_pdf_download_link(page, book_url)
                
                if pdf_info:
                    download_list.append(pdf_info)
                    
                await asyncio.sleep(1)
            
            await browser.close()
            
            print(f"\n✓ Found {len(download_list)} PDFs to download")
            
            if len(download_list) == 0:
                print("⚠ No PDFs found to download!")
                return
            
            print(f"\n[3/3] Downloading {len(download_list)} PDFs...")
            successful = 0
            
            async with aiohttp.ClientSession() as session:
                for idx, info in enumerate(download_list, 1):
                    result = await self.download_file(session, info, idx, len(download_list))
                    if result:
                        successful += 1
            
            print("\n" + "=" * 70)
            print(f"✓ Process complete!")
            print(f"  Books found: {len(book_links)}")
            print(f"  PDFs available: {len(download_list)}")
            print(f"  Successfully downloaded: {successful}")
            print(f"  Location: {self.download_dir.absolute()}")
            print("=" * 70)

if __name__ == "__main__":
    scraper = VTechWorksBookScraper(download_dir="vtechworks_books")
    asyncio.run(scraper.run())
