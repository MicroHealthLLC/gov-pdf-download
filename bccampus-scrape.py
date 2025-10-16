import asyncio
from playwright.async_api import async_playwright
from pathlib import Path
import re
import aiohttp
import aiofiles
from urllib.parse import urlparse

async def download_pdf(session, url, save_path, book_title, max_retries=3):
    """
    Download PDF directly using aiohttp with retry logic
    """
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/pdf,application/octet-stream,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://collection.bccampus.ca/'
            }
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=180), allow_redirects=True) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Verify it's actually a PDF
                    if content[:4] == b'%PDF':
                        async with aiofiles.open(save_path, 'wb') as f:
                            await f.write(content)
                        print(f"    ✓ Downloaded: {book_title}.pdf ({len(content) / 1024 / 1024:.2f} MB)")
                        return True
                    else:
                        print(f"    ✗ Downloaded file is not a valid PDF: {book_title}")
                        return False
                        
                elif response.status == 403:
                    print(f"    ✗ Access denied (403 Forbidden): {book_title}")
                    print(f"      URL: {url}")
                    return False
                    
                elif response.status == 404:
                    print(f"    ✗ File not found (404): {book_title}")
                    return False
                    
                else:
                    if attempt < max_retries - 1:
                        print(f"    ⚠ Status {response.status}, retrying ({attempt + 1}/{max_retries})...")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        print(f"    ✗ Failed to download (Status {response.status}): {book_title}")
                        return False
                        
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                print(f"    ⚠ Timeout, retrying ({attempt + 1}/{max_retries})...")
                await asyncio.sleep(2 ** attempt)
                continue
            else:
                print(f"    ✗ Timeout after {max_retries} attempts: {book_title}")
                return False
                
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"    ⚠ Error: {str(e)}, retrying ({attempt + 1}/{max_retries})...")
                await asyncio.sleep(2 ** attempt)
                continue
            else:
                print(f"    ✗ Error downloading {book_title}: {str(e)}")
                return False
    
    return False

async def scrape_bccampus_pdfs():
    """
    Scrape PDFs from BC Campus Open Collection organized by subject
    Intercepts network requests to capture signed URLs with authentication parameters
    """
    base_url = "https://collection.bccampus.ca"
    download_dir = Path("bccampus_downloads")
    download_dir.mkdir(exist_ok=True)
    
    # Track statistics
    stats = {
        'total_books': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0
    }
    
    # Create error log file
    error_log = download_dir / "download_errors.log"
    
    # Create aiohttp session for downloads
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as http_session:
        async with async_playwright() as p:
            # Launch browser in HEADLESS mode
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            print(f"Starting scrape of {base_url}")
            
            # Navigate to landing page
            try:
                await page.goto(base_url, wait_until='domcontentloaded', timeout=60000)
                await asyncio.sleep(3)
            except Exception as e:
                print(f"Error loading landing page: {e}")
                await browser.close()
                return
            
            # Extract all subject categories
            subject_elements = await page.query_selector_all('a[href*="/concept/subject/"]')
            
            subject_data = []
            for subject_elem in subject_elements:
                try:
                    subject_name = await subject_elem.inner_text()
                    subject_link = await subject_elem.get_attribute('href')
                    
                    if subject_link and '/concept/subject/' in subject_link:
                        clean_name = re.sub(r'[<>:"/\\|?*]', '', subject_name.strip())
                        clean_name = re.sub(r'\d+\s+results?', '', clean_name).strip()
                        
                        if clean_name and len(clean_name) > 0:
                            full_url = base_url + subject_link if not subject_link.startswith('http') else subject_link
                            subject_data.append({
                                'name': clean_name,
                                'url': full_url
                            })
                except Exception:
                    continue
            
            # Remove duplicates
            seen_urls = set()
            unique_subjects = []
            for subject in subject_data:
                if subject['url'] not in seen_urls:
                    seen_urls.add(subject['url'])
                    unique_subjects.append(subject)
            
            print(f"Found {len(unique_subjects)} unique subjects\n")
            
            # Process each subject
            for idx, subject in enumerate(unique_subjects, 1):
                print(f"[{idx}/{len(unique_subjects)}] Processing subject: {subject['name']}")
                
                subject_folder = download_dir / subject['name']
                subject_folder.mkdir(exist_ok=True)
                
                try:
                    await page.goto(subject['url'], wait_until='domcontentloaded', timeout=60000)
                    await asyncio.sleep(2)
                    
                    book_elements = await page.query_selector_all('a[href*="/textbook/"]')
                    
                    book_urls = []
                    for book_elem in book_elements:
                        book_url = await book_elem.get_attribute('href')
                        if book_url:
                            full_url = base_url + book_url if not book_url.startswith('http') else book_url
                            if full_url not in book_urls:
                                book_urls.append(full_url)
                    
                    print(f"  Found {len(book_urls)} books")
                    stats['total_books'] += len(book_urls)
                    
                    # Process each book
                    for book_idx, book_url in enumerate(book_urls, 1):
                        try:
                            print(f"  [{book_idx}/{len(book_urls)}] Processing: {book_url}")
                            
                            await page.goto(book_url, wait_until='domcontentloaded', timeout=60000)
                            await asyncio.sleep(2)
                            
                            # Get book title
                            title_elem = await page.query_selector('h1')
                            book_title = await title_elem.inner_text() if title_elem else f"book_{book_idx}"
                            book_title = re.sub(r'[<>:"/\\|?*]', '', book_title.strip())[:100]
                            
                            save_path = subject_folder / f"{book_title}.pdf"
                            
                            if save_path.exists():
                                print(f"    ⊘ Already exists: {book_title}.pdf")
                                stats['skipped'] += 1
                                continue
                            
                            # Find PDF link
                            pdf_link = await page.query_selector('a[href*="download?type=pdf"], a[href$=".pdf"], a:has-text("PDF (.pdf)"), a:has-text("PDF"), button:has-text("PDF")')
                            
                            if pdf_link:
                                # CAPTURE THE ACTUAL DOWNLOAD URL BY INTERCEPTING THE REQUEST
                                captured_url = None
                                
                                async def capture_request(request):
                                    nonlocal captured_url
                                    # Capture PDF download requests
                                    if '.pdf' in request.url or 'download?type=pdf' in request.url:
                                        captured_url = request.url
                                
                                # Set up request interception
                                page.on("request", capture_request)
                                
                                # Click the link to trigger the download request
                                try:
                                    await pdf_link.click(timeout=5000)
                                    await asyncio.sleep(2)  # Wait for request to be captured
                                except Exception:
                                    pass  # Click might fail but request should still be captured
                                
                                # Remove the listener
                                page.remove_listener("request", capture_request)
                                
                                if captured_url:
                                    print(f"    Found PDF URL: {captured_url}")
                                    
                                    success = await download_pdf(http_session, captured_url, save_path, book_title)
                                    if success:
                                        stats['successful'] += 1
                                    else:
                                        stats['failed'] += 1
                                        async with aiofiles.open(error_log, 'a') as f:
                                            await f.write(f"{book_title}|{book_url}|{captured_url}\n")
                                else:
                                    print(f"    ✗ Could not capture PDF URL for: {book_title}")
                                    stats['failed'] += 1
                            else:
                                print(f"    ✗ No PDF link element found for: {book_title}")
                                stats['failed'] += 1
                            
                            await asyncio.sleep(2)
                            
                        except Exception as e:
                            print(f"    ✗ Error processing book: {str(e)}")
                            stats['failed'] += 1
                            continue
                            
                except Exception as e:
                    print(f"  ✗ Error processing subject: {str(e)}")
                    continue
            
            await browser.close()
            
            # Print summary
            print("\n" + "=" * 50)
            print("✓ Scraping complete!")
            print(f"Files saved to: {download_dir.absolute()}")
            print(f"\nStatistics:")
            print(f"  Total books found: {stats['total_books']}")
            print(f"  Successfully downloaded: {stats['successful']}")
            print(f"  Failed downloads: {stats['failed']}")
            print(f"  Already existed (skipped): {stats['skipped']}")
            print(f"\nFailed downloads logged to: {error_log}")

if __name__ == "__main__":
    print("BC Campus PDF Scraper - Network Request Interception")
    print("=" * 50)
    asyncio.run(scrape_bccampus_pdfs())
