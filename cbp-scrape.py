import asyncio
from playwright.async_api import async_playwright
import aiohttp
import os
from urllib.parse import urljoin
import re

async def download_file(session, url, filepath, headers):
    """Download a single file asynchronously."""
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                with open(filepath, 'wb') as f:
                    f.write(await response.read())
                return True
            else:
                print(f"Failed to download {url}: Status {response.status}")
                return False
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return False

async def scrape_cbp_files_with_pagination():
    """
    Scrape all PDF files from CBP policies page across all 54 pages.
    """
    base_url = "https://www.cbp.gov"
    start_url = "https://www.cbp.gov/newsroom/accountability-and-transparency/policies-procedures-and-directives"
    
    # Create download directory
    download_dir = "cbp_policy_files"
    os.makedirs(download_dir, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    all_pdf_links = {}  # Use dict to track unique URLs
    total_pages = 54  # As specified
    
    async with async_playwright() as p:
        # Launch browser (headless mode)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        print(f"Starting scrape of {start_url}")
        print(f"Total pages to process: {total_pages}\n")
        
        try:
            # Loop through all pages (0 to 53)
            for page_num in range(total_pages):
                # CREATE A NEW PAGE FOR EACH ITERATION to avoid DOM accumulation
                page = await context.new_page()
                
                # Construct URL with page parameter
                if page_num == 0:
                    page_url = start_url
                else:
                    page_url = f"{start_url}?page={page_num}"
                
                print(f"{'='*60}")
                print(f"Processing page {page_num + 1} of {total_pages}")
                print(f"URL: {page_url}")
                print(f"{'='*60}")
                
                try:
                    # Navigate to the page
                    await page.goto(page_url, wait_until='networkidle', timeout=30000)
                    await page.wait_for_timeout(2000)  # Wait for content to load
                    
                    # Wait for table to load
                    await page.wait_for_selector('table.usa-table', timeout=10000)
                    
                    # Extract PDF links from current page
                    pdf_links = await page.evaluate('''() => {
                        const links = [];
                        const table = document.querySelector('table.usa-table');
                        if (table) {
                            const tbody = table.querySelector('tbody');
                            if (tbody) {
                                const rows = tbody.querySelectorAll('tr');
                                rows.forEach(row => {
                                    // Get ALL cells including th
                                    const allCells = row.querySelectorAll('th, td');
                                    
                                    if (allCells.length >= 3) {
                                        // Title is in the first cell (th)
                                        const titleCell = allCells[0];
                                        const title = titleCell ? titleCell.innerText.trim() : 'Unknown';
                                        
                                        // File Download is in the THIRD cell (index 2)
                                        const fileCell = allCells[2];
                                        const anchors = fileCell.querySelectorAll('a[href]');
                                        
                                        anchors.forEach(anchor => {
                                            const href = anchor.getAttribute('href');
                                            const linkText = anchor.innerText.trim();
                                            
                                            // Check if it's a PDF link
                                            if (href && (href.toLowerCase().includes('.pdf') || linkText.toLowerCase().includes('pdf'))) {
                                                links.push({
                                                    url: href,
                                                    title: title,
                                                    filename: href.split('/').pop(),
                                                    linkText: linkText
                                                });
                                            }
                                        });
                                    }
                                });
                            }
                        }
                        return links;
                    }''')
                    
                    print(f"Found {len(pdf_links)} PDF files on page {page_num + 1}")
                    
                    # Add to master dict with URL as key to prevent duplicates
                    for link in pdf_links:
                        full_url = urljoin(base_url, link['url'])
                        if full_url not in all_pdf_links:  # Only add if not already present
                            link['url'] = full_url
                            link['page_number'] = page_num + 1
                            all_pdf_links[full_url] = link
                    
                    print(f"Total unique PDFs so far: {len(all_pdf_links)}")
                    
                    # CLOSE THE PAGE to free memory and avoid accumulation
                    await page.close()
                    
                    # Small delay between pages to be respectful
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"Error processing page {page_num + 1}: {str(e)}")
                    await page.close()
                    continue
            
            await browser.close()
            
        except Exception as e:
            print(f"Error during scraping: {str(e)}")
            await browser.close()
            return
    
    # Convert dict to list
    all_pdf_links_list = list(all_pdf_links.values())
    
    print(f"\n{'='*60}")
    print(f"Total unique PDF files found: {len(all_pdf_links_list)}")
    print(f"{'='*60}\n")
    
    if len(all_pdf_links_list) == 0:
        print("WARNING: No PDF files were found. Please check the page structure.")
        return
    
    # Save metadata to CSV for reference
    import csv
    metadata_file = os.path.join(download_dir, 'download_metadata.csv')
    with open(metadata_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['filename', 'title', 'url', 'page_number', 'linkText']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for link in all_pdf_links_list:
            writer.writerow(link)
    print(f"Metadata saved to: {metadata_file}\n")
    
    # Download files
    async with aiohttp.ClientSession() as session:
        for idx, file_info in enumerate(all_pdf_links_list, 1):
            # Clean filename to avoid issues
            filename = re.sub(r'[<>:"/\\|?*]', '_', file_info['filename'])
            filepath = os.path.join(download_dir, filename)
            
            if os.path.exists(filepath):
                print(f"[{idx}/{len(all_pdf_links_list)}] ✓ Already exists: {filename}")
                continue
            
            print(f"[{idx}/{len(all_pdf_links_list)}] Downloading: {filename}")
            success = await download_file(session, file_info['url'], filepath, headers)
            
            if success:
                print(f"[{idx}/{len(all_pdf_links_list)}] ✓ Saved: {filename}")
            else:
                print(f"[{idx}/{len(all_pdf_links_list)}] ✗ Failed: {filename}")
            
            await asyncio.sleep(0.5)  # Respectful delay
    
    print(f"\n✓ Complete! Downloaded {len(all_pdf_links_list)} files to '{download_dir}' directory")

if __name__ == "__main__":
    asyncio.run(scrape_cbp_files_with_pagination())