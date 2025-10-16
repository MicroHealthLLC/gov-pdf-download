import asyncio
from playwright.async_api import async_playwright
from pathlib import Path

async def scrape_osu_textbooks():
    """
    Scrapes Oregon State Open Education textbooks catalog
    and downloads all available PDFs in headless mode
    """
    
    # Create downloads directory
    download_dir = Path("./osu_textbooks")
    download_dir.mkdir(exist_ok=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        context = await browser.new_context(
            accept_downloads=True
        )
        page = await context.new_page()
        
        all_book_urls = []
        
        # Step 1: Scrape all book URLs from paginated catalog
        print("Collecting book URLs from catalog pages...")
        for page_num in range(1, 7):
            catalog_url = f"https://open.oregonstate.education/?pg={page_num}#catalog"
            print(f"Scraping catalog page {page_num}...")
            
            await page.goto(catalog_url, wait_until='networkidle')
            await page.wait_for_timeout(2000)
            
            # Extract book links - adjust selector based on actual HTML
            book_links = await page.eval_on_selector_all(
                'a[href*="open.oregonstate.edu"]',
                '''(elements) => elements
                    .map(el => el.href)
                    .filter(href => 
                        href.includes('open.oregonstate.education/') && 
                        !href.includes('?pg=') && 
                        !href.includes('#catalog') &&
                        !href.includes('/h5p-listing') &&
                        href !== 'https://open.oregonstate.education/'
                    )
                '''
            )
            
            unique_links = list(set(book_links))
            all_book_urls.extend(unique_links)
            print(f"Found {len(unique_links)} books on page {page_num}")
        
        all_book_urls = list(set(all_book_urls))
        print(f"\nTotal unique books found: {len(all_book_urls)}\n")
        
        # Step 2: Download PDFs from each book page
        for idx, book_url in enumerate(all_book_urls, 1):
            try:
                print(f"[{idx}/{len(all_book_urls)}] Processing: {book_url}")
                
                # Navigate to book page first
                await page.goto(book_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(1000)
                
                # Extract book title for filename
                title = await page.title()
                safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_title = safe_title[:100] or "untitled"
                
                # Construct download URL
                download_url = f"{book_url.rstrip('/')}/open/download?type=pdf"
                print(f"  Downloading from: {download_url}")
                
                # METHOD 1: Use expect_download with JavaScript trigger
                async with page.expect_download(timeout=120000) as download_info:
                    # Trigger download via JavaScript instead of page.goto
                    await page.evaluate(f"""
                        () => {{
                            window.location.href = '{download_url}';
                        }}
                    """)
                
                download = await download_info.value
                
                # Save with custom filename
                file_path = download_dir / f"{safe_title}.pdf"
                await download.save_as(file_path)
                print(f"  ✓ Saved: {file_path.name}\n")
                
                # Wait a bit between downloads to be respectful
                await page.wait_for_timeout(1000)
                
            except Exception as e:
                print(f"  ✗ Error downloading {book_url}: {str(e)}\n")
                continue
        
        await browser.close()
        print(f"\n✓ Download complete! Files saved to: {download_dir.absolute()}")

if __name__ == "__main__":
    asyncio.run(scrape_osu_textbooks())