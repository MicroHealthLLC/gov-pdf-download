import asyncio
from playwright.async_api import async_playwright
import os
from urllib.parse import urljoin
import time

# Category URLs to scrape
CATEGORIES = [
    "https://open.umn.edu/opentextbooks/subjects/computer-science-information-systems",
    "https://open.umn.edu/opentextbooks/subjects/business",
    "https://open.umn.edu/opentextbooks/subjects/education",
    "https://open.umn.edu/opentextbooks/subjects/engineering",
    "https://open.umn.edu/opentextbooks/subjects/humanities",
    "https://open.umn.edu/opentextbooks/subjects/journalism-media-studies-communications",
    "https://open.umn.edu/opentextbooks/subjects/law",
    "https://open.umn.edu/opentextbooks/subjects/mathematics",
    "https://open.umn.edu/opentextbooks/subjects/medicine",
    "https://open.umn.edu/opentextbooks/subjects/natural-sciences",
    "https://open.umn.edu/opentextbooks/subjects/social-sciences"
]

# Domains we want to download from
ALLOWED_DOWNLOAD_DOMAINS = ['milneopentextbooks.org', 'open.umn.edu']

# Download directory
DOWNLOAD_DIR = "./textbook_pdfs"

# Base URL for resolving relative links
BASE_URL = "https://open.umn.edu"

async def setup_download_dir():
    """Create download directory if it doesn't exist"""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

async def is_allowed_domain(url):
    """Check if URL is from an allowed download domain"""
    if not url:
        return False
    return any(domain in url for domain in ALLOWED_DOWNLOAD_DOMAINS)

async def scrape_books():
    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            accept_downloads=True
        )
        page = await context.new_page()
        
        await setup_download_dir()
        
        downloaded_count = 0
        skipped_count = 0
        error_count = 0
        
        # Iterate through each category
        for category_url in CATEGORIES:
            print(f"\n{'='*60}")
            print(f"Processing category: {category_url}")
            print(f"{'='*60}")
            
            try:
                await page.goto(category_url, wait_until='networkidle', timeout=60000)
                await page.wait_for_timeout(2000)
                
                # Find all "Read More" buttons
                read_more_buttons = await page.locator('a:has-text("READ MORE")').all()
                print(f"Found {len(read_more_buttons)} books in this category")
                
                # Get all book URLs first and convert to absolute URLs
                book_urls = []
                for button in read_more_buttons:
                    href = await button.get_attribute('href')
                    if href:
                        # Convert relative URLs to absolute URLs
                        book_url = urljoin(BASE_URL, href)
                        book_urls.append(book_url)
                
                # Process each book
                for idx, book_url in enumerate(book_urls, 1):
                    print(f"\n[{idx}/{len(book_urls)}] Processing book: {book_url}")
                    
                    try:
                        # Navigate to book page
                        await page.goto(book_url, wait_until='networkidle', timeout=60000)
                        await page.wait_for_timeout(2000)
                        
                        # Get book title for filename
                        try:
                            title_element = await page.locator('h1, h2').first.text_content()
                            book_title = title_element.strip() if title_element else f"book_{idx}"
                        except:
                            book_title = f"book_{idx}"
                        
                        # Find PDF button
                        pdf_button = page.locator('a:has-text("PDF")').first
                        
                        if await pdf_button.count() > 0:
                            pdf_url = await pdf_button.get_attribute('href')
                            
                            # Convert relative URL to absolute if needed
                            if pdf_url:
                                pdf_url = urljoin(book_url, pdf_url)
                                print(f"  PDF URL: {pdf_url}")
                                
                                # Check if it's an allowed domain
                                if await is_allowed_domain(pdf_url):
                                    print(f"  ✓ Allowed domain detected, attempting download...")
                                    
                                    try:
                                        # Handle download
                                        async with page.expect_download(timeout=30000) as download_info:
                                            await pdf_button.click()
                                        
                                        # Wait for download to start
                                        await page.wait_for_timeout(5000)
                                        
                                        download = await download_info.value
                                        
                                        # Save with sanitized filename
                                        safe_title = "".join(c for c in book_title if c.isalnum() or c in (' ', '-', '_')).strip()
                                        safe_title = safe_title[:100]  # Limit length
                                        filename = f"{safe_title}.pdf"
                                        filepath = os.path.join(DOWNLOAD_DIR, filename)
                                        
                                        await download.save_as(filepath)
                                        print(f"  ✓ Downloaded: {filename}")
                                        downloaded_count += 1
                                        
                                    except Exception as e:
                                        print(f"  ✗ Download failed: {str(e)}")
                                        error_count += 1
                                    
                                else:
                                    print(f"  ✗ Skipped: External site (not in allowed domains)")
                                    skipped_count += 1
                            else:
                                print(f"  ✗ No PDF URL found")
                                skipped_count += 1
                        else:
                            print(f"  ✗ No PDF button found")
                            skipped_count += 1
                            
                    except Exception as e:
                        print(f"  ✗ Error processing book: {str(e)}")
                        error_count += 1
                        continue
                    
            except Exception as e:
                print(f"Error processing category {category_url}: {str(e)}")
                error_count += 1
                continue
        
        await browser.close()
        
        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"{'='*60}")
        print(f"Total downloaded: {downloaded_count}")
        print(f"Total skipped: {skipped_count}")
        print(f"Total errors: {error_count}")
        print(f"Download directory: {DOWNLOAD_DIR}")

# Run the scraper
if __name__ == "__main__":
    asyncio.run(scrape_books())