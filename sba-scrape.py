import asyncio
import os
import re
from pathlib import Path
from playwright.async_api import async_playwright
import requests
from urllib.parse import urljoin, urlparse
import mimetypes

async def download_document(page, doc_url, download_dir, doc_number):
    """
    Download any type of document by extracting direct download links
    """
    try:
        print(f"\n[{doc_number}] Processing: {doc_url}")
        
        # Navigate to document page
        await page.goto(doc_url, wait_until="networkidle", timeout=30000)
        
        # Wait a moment for page to fully load
        await asyncio.sleep(1)
        
        # Extract ALL download links from the page
        # Look for links with common file extensions or download buttons
        download_links = await page.evaluate('''() => {
            const links = [];
            
            // Method 1: Find all <a> tags with download-related classes or text
            const downloadButtons = document.querySelectorAll('a.usa-button, a[class*="download"], a[href*="download"]');
            downloadButtons.forEach(link => {
                const href = link.getAttribute('href');
                const text = link.textContent.trim();
                if (href) {
                    links.push({
                        url: href,
                        text: text,
                        type: 'button'
                    });
                }
            });
            
            // Method 2: Find all links to files with common extensions
            const fileLinks = document.querySelectorAll('a[href$=".pdf"], a[href$=".xlsx"], a[href$=".xls"], a[href$=".docx"], a[href$=".doc"], a[href$=".pptx"], a[href$=".ppt"], a[href$=".zip"], a[href$=".csv"]');
            fileLinks.forEach(link => {
                const href = link.getAttribute('href');
                const text = link.textContent.trim();
                if (href && !links.some(l => l.url === href)) {
                    links.push({
                        url: href,
                        text: text,
                        type: 'direct'
                    });
                }
            });
            
            // Method 3: Look in the document download section specifically
            const downloadSection = document.querySelector('.sba-document__download');
            if (downloadSection) {
                const sectionLinks = downloadSection.querySelectorAll('a[href]');
                sectionLinks.forEach(link => {
                    const href = link.getAttribute('href');
                    const text = link.textContent.trim();
                    if (href && !links.some(l => l.url === href)) {
                        links.push({
                            url: href,
                            text: text,
                            type: 'section'
                        });
                    }
                });
            }
            
            return links;
        }''')
        
        if not download_links or len(download_links) == 0:
            print(f"  ❌ No download links found")
            return False, None
        
        print(f"  Found {len(download_links)} download link(s)")
        
        # Download all files found
        downloaded_files = []
        
        for idx, link_info in enumerate(download_links, 1):
            try:
                file_url = link_info['url']
                link_text = link_info['text']
                
                # Make absolute URL
                file_url = urljoin("https://www.sba.gov", file_url)
                
                # Extract filename and extension
                parsed_url = urlparse(file_url)
                path_parts = parsed_url.path.split('/')
                filename = path_parts[-1] if path_parts else None
                
                # Clean up filename (remove URL encoding)
                if filename:
                    filename = requests.utils.unquote(filename)
                
                # Detect file extension
                file_extension = None
                if filename and '.' in filename:
                    file_extension = filename.split('.')[-1].lower()
                else:
                    # Try to detect from link text
                    text_lower = link_text.lower()
                    if 'pdf' in text_lower or '.pdf' in file_url.lower():
                        file_extension = 'pdf'
                    elif 'xlsx' in text_lower or '.xlsx' in file_url.lower():
                        file_extension = 'xlsx'
                    elif 'xls' in text_lower or '.xls' in file_url.lower():
                        file_extension = 'xls'
                    elif 'docx' in text_lower or '.docx' in file_url.lower():
                        file_extension = 'docx'
                    elif 'doc' in text_lower or '.doc' in file_url.lower():
                        file_extension = 'doc'
                    elif 'pptx' in text_lower or '.pptx' in file_url.lower():
                        file_extension = 'pptx'
                    elif 'ppt' in text_lower or '.ppt' in file_url.lower():
                        file_extension = 'ppt'
                
                # Create filename if needed
                if not filename or not file_extension:
                    # Get document title
                    title_element = await page.query_selector('h1')
                    if title_element:
                        title_text = await title_element.inner_text()
                        clean_title = re.sub(r'[^\w\s-]', '', title_text)
                        clean_title = re.sub(r'[-\s]+', '-', clean_title)
                        filename = f"{clean_title[:100]}.{file_extension or 'bin'}"
                    else:
                        filename = f"document_{doc_number}_{idx}.{file_extension or 'bin'}"
                
                # Ensure proper extension
                if file_extension and not filename.endswith(f'.{file_extension}'):
                    filename = f"{filename}.{file_extension}"
                
                filepath = download_dir / filename
                
                # Skip if already exists
                if filepath.exists():
                    file_size = filepath.stat().st_size / 1024
                    print(f"  ⏭️  [{idx}/{len(download_links)}] Skipped (exists): {filename} ({file_size:.1f} KB)")
                    downloaded_files.append(filename)
                    continue
                
                print(f"  📥 [{idx}/{len(download_links)}] Downloading: {filename}")
                print(f"      URL: {file_url}")
                
                # Download file
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.get(file_url, stream=True, timeout=30, headers=headers)
                response.raise_for_status()
                
                # Try to get better extension from content-type if needed
                if not file_extension or file_extension == 'bin':
                    content_type = response.headers.get('content-type', '')
                    ext = mimetypes.guess_extension(content_type.split(';')[0])
                    if ext:
                        filename = filename.replace('.bin', ext)
                        filepath = download_dir / filename
                
                # Write file
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                file_size = filepath.stat().st_size / 1024
                print(f"  ✅ [{idx}/{len(download_links)}] Downloaded: {filename} ({file_size:.1f} KB)")
                downloaded_files.append(filename)
                
            except Exception as e:
                print(f"  ⚠️  Error downloading link {idx}: {str(e)}")
                continue
        
        if downloaded_files:
            return True, downloaded_files
        else:
            print(f"  ❌ No files successfully downloaded")
            return False, None
            
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        return False, None


async def download_sba_documents():
    """
    Download all documents from SBA documents page
    """
    download_dir = Path("sba_documents")
    download_dir.mkdir(exist_ok=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        base_url = "https://www.sba.gov/documents"
        params = "?query=&type=All&program=All&office=All&activity=All&sort_by=last_updated"
        await page.goto(f"{base_url}{params}", wait_until="networkidle")
        
        downloaded_count = 0
        failed_downloads = []
        current_page = 1
        total_documents_processed = 0
        total_files_downloaded = 0
        
        while True:
            print(f"\n{'='*60}")
            print(f"📄 SCRAPING PAGE {current_page}")
            print(f"{'='*60}")
            
            try:
                await page.wait_for_selector('a[href*="/document/"]', timeout=10000)
            except Exception as e:
                print(f"No documents found on page {current_page}: {str(e)}")
                break
            
            document_links = await page.evaluate('''() => {
                const links = Array.from(document.querySelectorAll('a[href*="/document/"]'));
                return links
                    .map(link => link.href)
                    .filter(href => href.includes('/document/support') || href.includes('/document/sba'));
            }''')
            
            unique_links = list(set(document_links))
            print(f"Found {len(unique_links)} documents on page {current_page}")
            
            print(f"\n🔽 DOWNLOADING DOCUMENTS FROM PAGE {current_page}")
            print(f"{'='*60}")
            
            for idx, doc_url in enumerate(unique_links, 1):
                total_documents_processed += 1
                success, files = await download_document(
                    page, 
                    doc_url, 
                    download_dir, 
                    total_documents_processed
                )
                
                if success:
                    downloaded_count += 1
                    if files:
                        total_files_downloaded += len(files) if isinstance(files, list) else 1
                else:
                    failed_downloads.append(doc_url)
                
                await asyncio.sleep(1)
            
            print(f"\n{'='*60}")
            print(f"📊 PAGE {current_page} SUMMARY")
            print(f"{'='*60}")
            print(f"  Documents on this page: {len(unique_links)}")
            print(f"  Total documents processed: {total_documents_processed}")
            print(f"  Total files downloaded: {total_files_downloaded}")
            print(f"  Total successful: {downloaded_count}")
            print(f"  Total failed: {len(failed_downloads)}")
            print(f"{'='*60}")
            
            await page.goto(f"{base_url}{params}&page={current_page}", wait_until="networkidle")
            
            next_button = None
            
            try:
                next_button = await page.query_selector('a:has-text("Next")')
                
                if not next_button:
                    next_button = await page.query_selector('a[rel="next"]')
                
                if not next_button:
                    next_button = await page.query_selector('nav a:has-text(">")')
                
                if next_button:
                    is_disabled = await next_button.evaluate('''(element) => {
                        return element.classList.contains('disabled') || 
                               element.getAttribute('aria-disabled') === 'true' ||
                               element.hasAttribute('disabled');
                    }''')
                    
                    if not is_disabled:
                        print(f"\n➡️  Navigating to page {current_page + 1}...")
                        await next_button.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(2)
                        current_page += 1
                    else:
                        print("\n🏁 Next button is disabled. Reached last page.")
                        break
                else:
                    print("\n🏁 No 'Next' button found. Reached last page.")
                    break
                    
            except Exception as e:
                print(f"\n⚠️  Error navigating to next page: {str(e)}")
                break
        
        await browser.close()
        
        print(f"\n\n{'='*60}")
        print("🎉 FINAL DOWNLOAD SUMMARY")
        print(f"{'='*60}")
        print(f"  Pages scraped: {current_page}")
        print(f"  Total documents processed: {total_documents_processed}")
        print(f"  Total files downloaded: {total_files_downloaded}")
        print(f"  Successfully processed: {downloaded_count}")
        print(f"  Failed downloads: {len(failed_downloads)}")
        if total_documents_processed > 0:
            print(f"  Success rate: {(downloaded_count/total_documents_processed*100):.1f}%")
        print(f"  Download directory: {download_dir.absolute()}")
        print(f"{'='*60}")
        
        if failed_downloads:
            print(f"\n❌ Failed URLs ({len(failed_downloads)} total):")
            for url in failed_downloads[:10]:
                print(f"  - {url}")
            if len(failed_downloads) > 10:
                print(f"  ... and {len(failed_downloads) - 10} more")

if __name__ == "__main__":
    asyncio.run(download_sba_documents())