import asyncio
from playwright.async_api import async_playwright
from pathlib import Path
import re
import aiohttp
import aiofiles

async def download_pdf(session, url, save_path, book_title):
    """
    Download PDF directly using aiohttp
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=180)) as response:
            if response.status == 200:
                content = await response.read()
                async with aiofiles.open(save_path, 'wb') as f:
                    await f.write(content)
                print(f"    ✓ Downloaded: {book_title}.pdf ({len(content) / 1024 / 1024:.2f} MB)")
                return True
            else:
                print(f"    ✗ Failed to download (Status {response.status}): {book_title}")
                return False
    except Exception as e:
        print(f"    ✗ Error downloading {book_title}: {str(e)}")
        return False

async def scrape_bccampus_pdfs():
    """
    Scrape PDFs from BC Campus Open Collection organized by subject
    Two-step process: Extract book page URL, then extract actual PDF URL
    """
    base_url = "https://collection.bccampus.ca"
    download_dir = Path("bccampus_downloads")
    download_dir.mkdir(exist_ok=True)
    
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
            
            # Extract all subject categories from the landing page
            subject_elements = await page.query_selector_all('a[href*="/concept/subject/"]')
            
            subject_data = []
            for subject_elem in subject_elements:
                try:
                    subject_name = await subject_elem.inner_text()
                    subject_link = await subject_elem.get_attribute('href')
                    
                    if subject_link and '/concept/subject/' in subject_link:
                        # Clean subject name for folder creation
                        clean_name = re.sub(r'[<>:"/\\|?*]', '', subject_name.strip())
                        clean_name = re.sub(r'\d+\s+results?', '', clean_name).strip()
                        
                        if clean_name and len(clean_name) > 0:
                            full_url = base_url + subject_link if not subject_link.startswith('http') else subject_link
                            subject_data.append({
                                'name': clean_name,
                                'url': full_url
                            })
                except Exception as e:
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
                
                # Create subject folder
                subject_folder = download_dir / subject['name']
                subject_folder.mkdir(exist_ok=True)
                
                try:
                    # Navigate to subject page
                    await page.goto(subject['url'], wait_until='domcontentloaded', timeout=60000)
                    await asyncio.sleep(2)
                    
                    # Find all book links on the subject page
                    book_elements = await page.query_selector_all('a[href*="/textbook/"]')
                    
                    book_urls = []
                    for book_elem in book_elements:
                        book_url = await book_elem.get_attribute('href')
                        if book_url:
                            full_url = base_url + book_url if not book_url.startswith('http') else book_url
                            if full_url not in book_urls:
                                book_urls.append(full_url)
                    
                    print(f"  Found {len(book_urls)} books")
                    
                    # Process each book
                    for book_idx, book_url in enumerate(book_urls, 1):
                        try:
                            print(f"  [{book_idx}/{len(book_urls)}] Processing: {book_url}")
                            
                            # STEP 1: Navigate to book page
                            await page.goto(book_url, wait_until='domcontentloaded', timeout=60000)
                            await asyncio.sleep(2)
                            
                            # Get book title for filename
                            title_elem = await page.query_selector('h1')
                            book_title = await title_elem.inner_text() if title_elem else f"book_{book_idx}"
                            book_title = re.sub(r'[<>:"/\\|?*]', '', book_title.strip())[:100]
                            
                            # STEP 2: Extract the ACTUAL PDF download URL from the page
                            # Try multiple selectors to find the PDF link
                            pdf_link = await page.query_selector('a[href*="download?type=pdf"], a[href$=".pdf"], a:has-text("PDF (.pdf)"), a:has-text("PDF"), button:has-text("PDF")')
                            
                            if pdf_link:
                                # Get the actual PDF URL
                                pdf_url = await pdf_link.get_attribute('href')
                                
                                if pdf_url:
                                    # Make absolute URL if needed
                                    if not pdf_url.startswith('http'):
                                        # Check if it's a relative URL from the book page domain
                                        if pdf_url.startswith('/'):
                                            # Extract base domain from book page
                                            current_url = page.url
                                            from urllib.parse import urlparse
                                            parsed = urlparse(current_url)
                                            pdf_url = f"{parsed.scheme}://{parsed.netloc}{pdf_url}"
                                        else:
                                            pdf_url = base_url + pdf_url
                                    
                                    print(f"    Found PDF URL: {pdf_url}")
                                    
                                    # Download the PDF
                                    save_path = subject_folder / f"{book_title}.pdf"
                                    
                                    # Skip if already downloaded
                                    if save_path.exists():
                                        print(f"    ⊘ Already exists: {book_title}.pdf")
                                    else:
                                        await download_pdf(http_session, pdf_url, save_path, book_title)
                                else:
                                    print(f"    ✗ No PDF URL attribute found for: {book_title}")
                            else:
                                print(f"    ✗ No PDF link element found for: {book_title}")
                            
                            # Respectful delay
                            await asyncio.sleep(2)
                            
                        except Exception as e:
                            print(f"    ✗ Error processing book: {str(e)}")
                            continue
                            
                except Exception as e:
                    print(f"  ✗ Error processing subject: {str(e)}")
                    continue
            
            await browser.close()
            print("\n✓ Scraping complete!")
            print(f"Files saved to: {download_dir.absolute()}")

# Run the scraper
if __name__ == "__main__":
    print("BC Campus PDF Scraper - Two-Step URL Extraction")
    print("=" * 50)
    asyncio.run(scrape_bccampus_pdfs())