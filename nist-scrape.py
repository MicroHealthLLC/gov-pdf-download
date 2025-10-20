import asyncio
import random
import aiohttp
from pathlib import Path
from playwright.async_api import async_playwright, Page

class NISTPublicationScraper:
    def __init__(self):
        # Single URL for ALL publications
        self.all_pubs_url = "https://www.nist.gov/publications/search?k=&t=&a=&ps=All&n=&d%5Bmin%5D=&d%5Bmax%5D="
        
    async def human_delay(self, min_sec=1, max_sec=3):
        """Simulate human-like delays"""
        await asyncio.sleep(random.uniform(min_sec, max_sec))
    
    async def download_pdf_http(self, pdf_url: str, download_path: str, title: str):
        """Download PDF using HTTP client"""
        try:
            folder = Path(download_path)
            folder.mkdir(parents=True, exist_ok=True)
            
            print(f"  📥 Downloading...")
            
            # Create safe filename
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()[:150]
            final_filename = f"{safe_title}.pdf"
            filepath = folder / final_filename
            
            # Skip if already exists
            if filepath.exists():
                print(f"  ⏭ Already exists: {final_filename}")
                return True
            
            # Download using aiohttp
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,*/*'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(pdf_url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        file_size_mb = len(content) / (1024 * 1024)
                        print(f"  ✓ Downloaded: {final_filename} ({file_size_mb:.2f} MB)")
                        await self.human_delay(1, 2)
                        return True
                    else:
                        print(f"  ✗ HTTP Error: {response.status}")
                        return False
            
        except Exception as e:
            print(f"  ✗ Download failed: {str(e)}")
            return False
    
    async def get_publication_links(self, page: Page):
        """Extract all publication links from current page"""
        try:
            await page.wait_for_selector('article.nist-teaser', timeout=10000)
            articles = await page.query_selector_all('article.nist-teaser')
            
            links = []
            for article in articles:
                link_elem = await article.query_selector('h3.nist-teaser__title a')
                if link_elem:
                    href = await link_elem.get_attribute('href')
                    title = await link_elem.inner_text()
                    if href and title:
                        full_url = f"https://www.nist.gov{href}" if href.startswith('/') else href
                        links.append({'url': full_url, 'title': title.strip()})
            
            return links
            
        except Exception as e:
            print(f"  ✗ Error extracting links: {e}")
            return []
    
    async def get_next_page_url(self, page: Page):
        """Get the URL for the next page"""
        try:
            # Look for pagination link with rel="next"
            next_link = await page.query_selector('nav.pager a[rel="next"]')
            if next_link:
                href = await next_link.get_attribute('href')
                if href:
                    return f"https://www.nist.gov{href}" if href.startswith('/') else href
        except Exception as e:
            print(f"  ⚠ Error finding next page: {e}")
        return None
    
    async def scrape_all_publications(self, page: Page, download_path: str):
        """Scrape ALL publications from NIST into single folder"""
        print(f"\n{'='*60}")
        print(f"Scraping ALL NIST Publications")
        print(f"Total Expected: ~73,697 documents")
        print(f"{'='*60}\n")
        
        downloaded_count = 0
        skipped_count = 0
        processed_urls = set()
        current_page_num = 1
        current_url = self.all_pubs_url
        
        while current_url:
            print(f"\n{'='*60}")
            print(f"PAGE {current_page_num}")
            print(f"{'='*60}")
            print(f"Loading: {current_url}")
            
            # Navigate to current page
            await page.goto(current_url, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            
            # Get all publication links on current page
            pub_links = await self.get_publication_links(page)
            
            if not pub_links:
                print(f"  ⚠ No publication links found on page {current_page_num}")
                break
            
            print(f"Found {len(pub_links)} publications on this page")
            print(f"Progress: {downloaded_count} downloaded, {skipped_count} skipped")
            
            # Process each publication
            for idx, pub in enumerate(pub_links, 1):
                url = pub['url']
                title = pub['title']
                
                # Skip if already processed
                if url in processed_urls:
                    continue
                
                processed_urls.add(url)
                
                try:
                    print(f"\n[Page {current_page_num} - {idx}/{len(pub_links)}] {title[:60]}...")
                    
                    # Navigate to publication page
                    await page.goto(url, wait_until="domcontentloaded")
                    await asyncio.sleep(2)
                    
                    # Find the "Local Download" link
                    local_download = await page.query_selector('a:has-text("Local Download")')
                    
                    if local_download:
                        pdf_url = await local_download.get_attribute('href')
                        
                        if pdf_url:
                            # Make URL absolute
                            if not pdf_url.startswith('http'):
                                pdf_url = f"https://tsapps.nist.gov{pdf_url}" if pdf_url.startswith('/') else f"https://tsapps.nist.gov/{pdf_url}"
                            
                            # Download the PDF
                            success = await self.download_pdf_http(pdf_url, download_path, title)
                            if success:
                                downloaded_count += 1
                        else:
                            print(f"  ⚠ No PDF URL found")
                            skipped_count += 1
                    else:
                        print(f"  ⚠ No download link found")
                        skipped_count += 1
                    
                except Exception as e:
                    print(f"  ✗ Error: {str(e)}")
                    skipped_count += 1
                    continue
            
            # Get next page URL
            await page.goto(current_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            
            next_url = await self.get_next_page_url(page)
            
            if next_url:
                print(f"\n  📄 Moving to next page...")
                current_url = next_url
                current_page_num += 1
                await self.human_delay(2, 4)
            else:
                print(f"\n  ℹ No more pages - completed all pages")
                break
        
        print(f"\n{'='*60}")
        print(f"✅ SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total Pages Processed: {current_page_num}")
        print(f"Documents Downloaded: {downloaded_count}")
        print(f"Documents Skipped: {skipped_count}")
        print(f"Total Processed: {downloaded_count + skipped_count}")
        print(f"{'='*60}\n")
        
        return downloaded_count
    
    async def run(self, download_path="./nist_all_publications"):
        """Main function - scrapes ALL NIST publications into single folder"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            await context.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            page = await context.new_page()
            
            # Scrape all publications
            total = await self.scrape_all_publications(page, download_path)
            
            print(f"\n🎉 ALL DONE! Downloaded {total} publications to: {download_path}")
            
            await browser.close()


async def main():
    scraper = NISTPublicationScraper()
    
    # Download ALL 73,697 publications into single folder
    await scraper.run(download_path="./nist_all_publications")


if __name__ == "__main__":
    asyncio.run(main())