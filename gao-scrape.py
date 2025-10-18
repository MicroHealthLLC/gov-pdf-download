import asyncio
from pathlib import Path
from playwright.async_api import async_playwright
from datetime import datetime
import re
import random

class GAOReportScraper:
    def __init__(self, output_folder="gao_reports"):
        """Initialize the scraper with output folder"""
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        self.base_url = "https://www.gao.gov"
        
    def build_gao_url(self, start_date_str, page_num=0):
        """Build GAO URL with dynamic date range and pagination"""
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.now()
            
            start_timestamp = int(start_date.replace(hour=23, minute=59, second=59).timestamp())
            end_timestamp = int(end_date.replace(hour=23, minute=59, second=59).timestamp())
            
            url = (
                f"{self.base_url}/reports-testimonies?"
                f"f%5B0%5D=rt_date_range_gui%3A%28min%3A{start_timestamp}%2Cmax%3A{end_timestamp}%29"
            )
            
            if page_num > 0:
                url += f"&page={page_num}"
            
            return url, start_date, end_date
            
        except ValueError as e:
            raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format. Error: {str(e)}")
    
    def is_valid_pdf(self, filepath):
        """Check if file is actually a PDF by reading header"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(4)
                return header == b'%PDF'
        except:
            return False
    
    async def download_pdf_properly(self, page, pdf_url, filename):
        """
        Download PDFs using fetch API - avoids timeout with expect_download()
        """
        try:
            filepath = self.output_folder / filename
            
            print(f"  Downloading PDF: {pdf_url}")
            
            # Method 1: Use fetch API (BEST for direct PDF URLs)
            # This avoids the timeout issue with expect_download()
            try:
                return await self.download_via_fetch(page, pdf_url, filepath, filename)
            except Exception as e:
                print(f"  Fetch method failed: {str(e)}")
            
            # Method 2: Use route interception
            try:
                return await self.download_with_route(page, pdf_url, filepath, filename)
            except Exception as e2:
                print(f"  Route method failed: {str(e2)}")
            
            # Method 3: Navigate and extract from response
            try:
                return await self.download_via_response(page, pdf_url, filepath, filename)
            except Exception as e3:
                print(f"  Response method failed: {str(e3)}")
                
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error: {str(e)}")
            return False
    
    async def download_via_fetch(self, page, pdf_url, filepath, filename):
        """
        Use browser's fetch API to download PDF - MOST RELIABLE METHOD
        This executes in the browser context and avoids download event issues
        """
        print(f"  Trying fetch method...")
        
        try:
            # Execute fetch in browser context to get PDF data
            result = await page.evaluate("""
                async (url) => {
                    try {
                        const response = await fetch(url, {
                            method: 'GET',
                            credentials: 'include',
                            headers: {
                                'Accept': 'application/pdf'
                            }
                        });
                        
                        if (!response.ok) {
                            return {
                                success: false,
                                error: `HTTP ${response.status}: ${response.statusText}`
                            };
                        }
                        
                        const blob = await response.blob();
                        const buffer = await blob.arrayBuffer();
                        const bytes = Array.from(new Uint8Array(buffer));
                        
                        return {
                            success: true,
                            data: bytes,
                            size: bytes.length
                        };
                    } catch (error) {
                        return {
                            success: false,
                            error: error.message
                        };
                    }
                }
            """, pdf_url)
            
            if not result['success']:
                print(f"  Fetch failed: {result.get('error', 'Unknown error')}")
                return False
            
            # Convert to bytes and save
            pdf_bytes = bytes(result['data'])
            
            with open(filepath, 'wb') as f:
                f.write(pdf_bytes)
            
            # Verify it's a valid PDF
            if self.is_valid_pdf(filepath):
                file_size = filepath.stat().st_size
                if file_size > 1000:
                    print(f"✓ Downloaded via fetch: {filename} ({file_size:,} bytes)")
                    return True
            
            print(f"  Invalid PDF or too small")
            if filepath.exists():
                filepath.unlink()
            return False
            
        except Exception as e:
            print(f"  Exception in fetch: {str(e)}")
            return False
    
    async def download_with_route(self, page, pdf_url, filepath, filename):
        """
        Intercept the request and save the response body directly
        """
        print(f"  Trying route interception method...")
        
        try:
            downloaded = False
            download_error = None
            
            async def handle_route(route):
                nonlocal downloaded, download_error
                try:
                    response = await route.fetch()
                    
                    # Get the body
                    body = await response.body()
                    
                    # Save it
                    with open(filepath, 'wb') as f:
                        f.write(body)
                    
                    downloaded = True
                    
                    # Continue the route
                    await route.fulfill(response=response)
                    
                except Exception as e:
                    download_error = str(e)
                    await route.abort()
            
            # Set up route handler
            await page.route(pdf_url, handle_route)
            
            # Navigate to trigger the route
            try:
                await page.goto(pdf_url, wait_until='commit', timeout=30000)
            except:
                pass  # Navigation might fail but route should have captured it
            
            # Wait a bit for the route to complete
            await asyncio.sleep(2)
            
            # Remove route
            await page.unroute(pdf_url)
            
            if download_error:
                print(f"  Route error: {download_error}")
                return False
            
            if downloaded and filepath.exists() and self.is_valid_pdf(filepath):
                file_size = filepath.stat().st_size
                if file_size > 1000:
                    print(f"✓ Downloaded via route: {filename} ({file_size:,} bytes)")
                    return True
            
            return False
            
        except Exception as e:
            print(f"  Route method exception: {str(e)}")
            return False
    
    async def download_via_response(self, page, pdf_url, filepath, filename):
        """
        Navigate and capture response directly
        """
        print(f"  Trying response capture method...")
        
        try:
            response_data = None
            
            async def handle_response(response):
                nonlocal response_data
                if pdf_url in response.url:
                    try:
                        response_data = await response.body()
                    except:
                        pass
            
            page.on('response', handle_response)
            
            await page.goto(pdf_url, wait_until='commit', timeout=30000)
            await asyncio.sleep(2)
            
            page.remove_listener('response', handle_response)
            
            if response_data:
                with open(filepath, 'wb') as f:
                    f.write(response_data)
                
                if self.is_valid_pdf(filepath):
                    file_size = filepath.stat().st_size
                    if file_size > 1000:
                        print(f"✓ Downloaded via response: {filename} ({file_size:,} bytes)")
                        return True
            
            return False
            
        except Exception as e:
            print(f"  Response method exception: {str(e)}")
            return False
    
    async def get_pdf_url_from_report_page(self, page, report_url):
        """Extract the PDF URL from a report page"""
        try:
            await page.goto(report_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            
            pdf_link_selector = 'a[href*=".pdf"]:has-text("Full Report")'
            await page.wait_for_selector(pdf_link_selector, timeout=10000)
            
            pdf_url = await page.locator(pdf_link_selector).get_attribute('href')
            
            if pdf_url and not pdf_url.startswith('http'):
                pdf_url = self.base_url + pdf_url
            
            return pdf_url
            
        except Exception as e:
            print(f"✗ Error extracting PDF URL: {str(e)}")
            return None
    
    async def get_total_pages(self, page):
        """Determine total number of pages from pagination"""
        try:
            pagination_selectors = [
                'nav.pager a',
                '.pager__item a',
                'ul.pagination li a',
                'a[href*="page="]'
            ]
            
            max_page = 0
            
            for selector in pagination_selectors:
                try:
                    links = await page.locator(selector).all()
                    for link in links:
                        href = await link.get_attribute('href')
                        if href and 'page=' in href:
                            match = re.search(r'page=(\d+)', href)
                            if match:
                                page_num = int(match.group(1))
                                max_page = max(max_page, page_num)
                except:
                    continue
            
            return max_page
            
        except Exception as e:
            print(f"Could not determine total pages: {str(e)}")
            return 0
    
    async def scrape_reports(self, start_date_str):
        """Main scraping function with pagination support"""
        
        async with async_playwright() as p:
            # Launch browser with anti-detection settings
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                ]
            )
            
            # Create context with proper configuration
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                accept_downloads=True,
                extra_http_headers={
                    'Accept': 'application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
            )
            
            # Add stealth modifications
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)
            
            page = await context.new_page()
            
            listing_url, start_date, end_date = self.build_gao_url(start_date_str, 0)
            
            print(f"\n{'='*70}")
            print(f"GAO Report Scraper")
            print(f"{'='*70}")
            print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            print(f"Output Folder: {self.output_folder.absolute()}")
            print(f"{'='*70}\n")
            
            try:
                print(f"Loading first page to check pagination...")
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3)
                
                page_title = await page.title()
                if "Access Denied" in page_title:
                    print(f"\n❌ ERROR: Access Denied by GAO website")
                    await browser.close()
                    return
                
                total_pages = await self.get_total_pages(page)
                print(f"✓ Found {total_pages + 1} pages to scrape\n")
                
            except Exception as e:
                print(f"✗ Error loading initial page: {str(e)}")
                await browser.close()
                return
            
            all_report_urls = []
            downloaded = 0
            failed = 0
            skipped = 0
            
            for page_num in range(total_pages + 1):
                listing_url, _, _ = self.build_gao_url(start_date_str, page_num)
                
                print(f"{'='*70}")
                print(f"Scraping Page {page_num + 1} of {total_pages + 1}")
                print(f"{'='*70}")
                
                try:
                    await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(random.uniform(2, 4))
                    
                    for _ in range(2):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)
                    
                    possible_selectors = [
                        'a[href*="/products/gao-"]',
                        'a[href*="/products/"]',
                        '.view-content a[href*="/products/"]',
                        'h3 a[href*="/products/"]'
                    ]
                    
                    report_links = []
                    for selector in possible_selectors:
                        try:
                            links = await page.locator(selector).all()
                            if len(links) > 0:
                                report_links = links
                                break
                        except:
                            continue
                    
                    page_report_urls = []
                    for link in report_links:
                        try:
                            href = await link.get_attribute('href')
                            if href and '/products/' in href:
                                full_url = self.base_url + href if not href.startswith('http') else href
                                if full_url not in all_report_urls:
                                    all_report_urls.append(full_url)
                                    page_report_urls.append(full_url)
                        except:
                            continue
                    
                    print(f"Found {len(page_report_urls)} reports on this page")
                    print(f"Total unique reports so far: {len(all_report_urls)}\n")
                    
                    for idx, report_url in enumerate(page_report_urls, 1):
                        print(f"[Page {page_num + 1}, Report {idx}/{len(page_report_urls)}] {report_url}")
                        
                        report_id = report_url.split('/')[-1]
                        filename = f"{report_id}.pdf"
                        
                        if (self.output_folder / filename).exists():
                            print(f"⊙ Already exists: {filename}\n")
                            skipped += 1
                            continue
                        
                        pdf_url = await self.get_pdf_url_from_report_page(page, report_url)
                        
                        if pdf_url:
                            success = await self.download_pdf_properly(page, pdf_url, filename)
                            if success:
                                downloaded += 1
                            else:
                                failed += 1
                        else:
                            print(f"✗ Could not find PDF URL")
                            failed += 1
                        
                        print()
                        # Random delay between downloads to avoid rate limiting
                        await asyncio.sleep(random.uniform(2, 5))
                    
                except Exception as e:
                    print(f"✗ Error on page {page_num + 1}: {str(e)}\n")
                    continue
            
            await browser.close()
            
            print(f"{'='*70}")
            print(f"Scraping Complete - All Pages")
            print(f"{'='*70}")
            print(f"Date Range:      {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            print(f"Total pages:     {total_pages + 1}")
            print(f"Downloaded:      {downloaded}")
            print(f"Already existed: {skipped}")
            print(f"Failed:          {failed}")
            print(f"Total processed: {len(all_report_urls)}")
            print(f"Files saved to:  {self.output_folder.absolute()}")
            print(f"{'='*70}\n")

def get_start_date_from_user():
    """Prompt user for start date with validation"""
    print("\n" + "="*70)
    print("GAO Report Scraper - Interactive Mode")
    print("="*70)
    print("\nThis tool will download all GAO reports from your specified")
    print("start date through today.\n")
    
    while True:
        start_date_input = input("Enter start date (YYYY-MM-DD, e.g., 2024-10-17): ").strip()
        
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(date_pattern, start_date_input):
            print("❌ Invalid format. Please use YYYY-MM-DD format (e.g., 2024-10-17)\n")
            continue
        
        try:
            start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
            
            if start_date > datetime.now():
                print("❌ Start date cannot be in the future. Please enter a valid date.\n")
                continue
            
            return start_date_input
            
        except ValueError:
            print("❌ Invalid date. Please enter a valid date (e.g., 2024-10-17)\n")

def main():
    start_date_str = get_start_date_from_user()
    end_date_str = datetime.now().strftime("%Y-%m-%d")
    output_folder = f"GAO-{start_date_str}-{end_date_str}"
    
    print(f"\n✓ Start date set to: {start_date_str}")
    print(f"✓ End date (today): {end_date_str}")
    print(f"✓ Output folder: {output_folder}\n")
    
    scraper = GAOReportScraper(output_folder=output_folder)
    asyncio.run(scraper.scrape_reports(start_date_str))

if __name__ == "__main__":
    main()