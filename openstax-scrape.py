import asyncio
import os
import re
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import requests

class OpenStaxPlaywrightDownloader:
    def __init__(self, base_url="https://openstax.org"):
        self.base_url = base_url
        self.download_folder = "OpenStax_Books"
        
        self.subjects = {
            "Business": "business",
            "College Success": "college-success",
            "Computer Science": "computer-science",
            "Humanities": "humanities",
            "Math": "math",
            "Nursing": "nursing",
            "Science": "science",
            "Social Sciences": "social-sciences"
        }
    
    def create_folder(self, folder_name):
        """Create folder if it doesn't exist"""
        Path(folder_name).mkdir(parents=True, exist_ok=True)
    
    def sanitize_filename(self, filename):
        """Remove invalid characters from filename"""
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = filename.strip()
        if len(filename) > 200:
            filename = filename[:200]
        return filename
    
    async def wait_for_page_load(self, page):
        """Wait for the SPA to fully load"""
        try:
            await page.wait_for_selector('#app > *', timeout=10000)
            await page.wait_for_load_state('networkidle', timeout=10000)
            await page.wait_for_timeout(2000)
        except PlaywrightTimeout:
            pass
    
    def download_pdf_with_requests(self, pdf_url, filepath):
        """Download PDF using requests library (more reliable)"""
        try:
            print(f"      → Downloading from: {pdf_url[:80]}...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            
            response = requests.get(pdf_url, headers=headers, stream=True, timeout=120)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = (downloaded / total_size) * 100
                            mb_downloaded = downloaded / 1024 / 1024
                            mb_total = total_size / 1024 / 1024
                            print(f"      Progress: {progress:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='\r')
                    
                    print(f"\n      ✓ Downloaded successfully ({mb_total:.2f} MB)")
            
            return True
            
        except Exception as e:
            print(f"\n      ✗ Download error: {str(e)}")
            if os.path.exists(filepath):
                os.remove(filepath)
            return False
    
    async def extract_pdf_from_page_content(self, page):
        """Extract PDF URL from page source"""
        try:
            content = await page.content()
            
            # Look for PDF URLs - prioritize actual book PDFs over guides
            pdf_patterns = [
                r'https://assets\.openstax\.org/oscms-prodcms/media/documents/[^"\']*?(?<!Instructor)[^"\']*?(?<!Student)[^"\']*?(?<!Getting)[^"\']*?(?<!Guide)\.pdf[^"\']*',
                r'https://assets\.openstax\.org/[^"\']+\.pdf[^"\']*',
                r'https://[^"\']*openstax[^"\']*\.pdf[^"\']*',
            ]
            
            found_pdfs = []
            
            for pattern in pdf_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    for match in matches:
                        # Filter out instructor/student guides
                        if not any(x in match.lower() for x in ['instructor', 'student', 'getting', 'guide', 'started']):
                            found_pdfs.append(match)
            
            # If we found PDFs, return the first one (likely the main book)
            if found_pdfs:
                pdf_url = found_pdfs[0]
                print(f"      → Found PDF: {pdf_url[:80]}...")
                return pdf_url
            
            # Fallback: return any PDF if no book PDF found
            all_pdfs = re.findall(r'https://[^"\']+\.pdf[^"\']*', content)
            if all_pdfs:
                for pdf in all_pdfs:
                    if 'openstax' in pdf.lower():
                        print(f"      → Found PDF (fallback): {pdf[:80]}...")
                        return pdf
            
            return None
            
        except Exception as e:
            print(f"      ✗ Error extracting PDF: {str(e)}")
            return None
    
    async def download_from_book_detail_page(self, page, book_url, subject_folder, idx, total):
        """Navigate to book detail page and download PDF"""
        try:
            print(f"\n  [{idx}/{total}] Accessing: {book_url}")
            
            # Navigate to book detail page
            await page.goto(book_url, wait_until='domcontentloaded', timeout=30000)
            await self.wait_for_page_load(page)
            
            # Get book title
            title_selectors = ['h1', 'h2', '[data-html="book-title"]']
            book_title = None
            
            for selector in title_selectors:
                try:
                    title_elem = await page.query_selector(selector)
                    if title_elem:
                        book_title = await title_elem.inner_text()
                        book_title = book_title.strip()
                        if len(book_title) > 5 and book_title != "Book details":
                            break
                except:
                    continue
            
            if not book_title or book_title == "Book details":
                # Extract from URL
                book_title = book_url.split('/')[-1].split('?')[0].replace('-', ' ').title()
            
            print(f"      Title: {book_title}")
            
            # Check if already downloaded
            filename = f"{self.sanitize_filename(book_title)}.pdf"
            filepath = os.path.join(subject_folder, filename)
            
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath) / 1024 / 1024
                print(f"      ⊘ Already exists ({file_size:.2f} MB)")
                return None
            
            # Extract PDF URL from page
            pdf_url = await self.extract_pdf_from_page_content(page)
            
            if not pdf_url:
                print(f"      ✗ Could not find PDF download link")
                return False
            
            # Clean up URL
            pdf_url = pdf_url.split('"')[0].split("'")[0]
            
            # Download using requests (more reliable than Playwright for direct file downloads)
            return self.download_pdf_with_requests(pdf_url, filepath)
            
        except Exception as e:
            print(f"      ✗ Error: {str(e)}")
            return False
    
    async def process_subject(self, page, subject_name, subject_slug):
        """Process all books in a subject"""
        print(f"\n  Navigating to: {self.base_url}/subjects/{subject_slug}")
        
        try:
            await page.goto(f"{self.base_url}/subjects/{subject_slug}", wait_until='domcontentloaded')
            await self.wait_for_page_load(page)
            
            # Find all links to book detail pages
            print(f"  → Looking for book detail page links...")
            book_links = await page.query_selector_all('a[href*="/details/books/"]')
            
            # Get unique URLs (filter out instructor/student resource links)
            book_urls = set()
            for link in book_links:
                href = await link.get_attribute('href')
                if href:
                    # Skip resource links
                    if '?' in href and any(x in href.lower() for x in ['instructor', 'student', 'resource']):
                        continue
                    
                    # Get base URL without query parameters
                    base_href = href.split('?')[0]
                    full_url = base_href if base_href.startswith('http') else f"{self.base_url}{base_href}"
                    book_urls.add(full_url)
            
            book_urls = list(book_urls)
            print(f"  Found {len(book_urls)} unique books")
            
            if not book_urls:
                return 0, 0, 0
            
            # Create subject folder
            subject_folder = os.path.join(self.download_folder, self.sanitize_filename(subject_name))
            self.create_folder(subject_folder)
            
            # Process each book
            downloaded = 0
            failed = 0
            skipped = 0
            
            for idx, book_url in enumerate(book_urls, 1):
                result = await self.download_from_book_detail_page(page, book_url, subject_folder, idx, len(book_urls))
                
                if result is True:
                    downloaded += 1
                elif result is False:
                    failed += 1
                else:
                    skipped += 1
                
                await page.wait_for_timeout(2000)
            
            return downloaded, failed, skipped
            
        except Exception as e:
            print(f"  ✗ Error processing subject: {str(e)}")
            return 0, 0, 0
    
    async def download_all(self):
        """Main function to download all PDFs"""
        print(f"\n{'='*70}")
        print("OpenStax PDF Downloader - Headless Mode")
        print(f"{'='*70}\n")
        
        self.create_folder(self.download_folder)
        
        total_downloaded = 0
        total_failed = 0
        total_skipped = 0
        
        async with async_playwright() as p:
            # Launch browser in HEADLESS mode (no window will appear)
            browser = await p.chromium.launch(
                headless=True,  # ← This ensures no browser window pops up
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            
            page = await context.new_page()
            
            # Process each subject
            for idx, (subject_name, subject_slug) in enumerate(self.subjects.items(), 1):
                print(f"\n{'='*70}")
                print(f"[{idx}/{len(self.subjects)}] SUBJECT: {subject_name}")
                print(f"{'='*70}")
                
                try:
                    downloaded, failed, skipped = await self.process_subject(page, subject_name, subject_slug)
                    
                    total_downloaded += downloaded
                    total_failed += failed
                    total_skipped += skipped
                    
                    print(f"\n{'─'*70}")
                    print(f"✓ Completed: {subject_name}")
                    print(f"  Downloaded: {downloaded} | Failed: {failed} | Skipped: {skipped}")
                    print(f"{'─'*70}")
                    
                    if idx < len(self.subjects):
                        print(f"\n⏳ Waiting 3 seconds before next subject...")
                        await page.wait_for_timeout(3000)
                        
                except Exception as e:
                    print(f"\n✗ Error processing subject {subject_name}: {str(e)}")
            
            await browser.close()
        
        print(f"\n{'='*70}")
        print("📊 FINAL SUMMARY")
        print(f"{'='*70}")
        print(f"✓ Successfully downloaded: {total_downloaded}")
        print(f"✗ Failed downloads: {total_failed}")
        print(f"⊘ Skipped (already exist): {total_skipped}")
        print(f"📁 Files saved to: {os.path.abspath(self.download_folder)}")
        print(f"{'='*70}\n")
        
        # Create download log
        log_file = os.path.join(self.download_folder, "download_log.txt")
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"OpenStax Download Log\n")
            f.write(f"{'='*50}\n")
            f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Downloaded: {total_downloaded}\n")
            f.write(f"Total Failed: {total_failed}\n")
            f.write(f"Total Skipped: {total_skipped}\n")
            f.write(f"{'='*50}\n")

async def main():
    print("Starting OpenStax PDF Downloader (Headless Mode)...")
    print("Running in background - no browser windows will appear.\n")
    
    downloader = OpenStaxPlaywrightDownloader()
    await downloader.download_all()
    
    print("\n✅ Download process complete!")

if __name__ == "__main__":
    import time
    asyncio.run(main())