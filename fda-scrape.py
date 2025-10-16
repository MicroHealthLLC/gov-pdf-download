import asyncio
import os
import re
from pathlib import Path
from urllib.parse import urljoin, unquote
from playwright.async_api import async_playwright
import requests
import logging
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FDACompliancePDFDownloader:
    def __init__(self, output_dir="fda_compliance_pdfs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Setup requests session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # FDA compliance program URLs
        self.urls = [
            {
                'url': 'https://www.fda.gov/vaccines-blood-biologics/enforcement-actions-cber/compliance-programs-cber',
                'category': 'vaccines_blood_biologics'
            },
            {
                'url': 'https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/compliance-program-manual/bioresearch-monitoring-program-bimo-compliance-programs',
                'category': 'bioresearch_monitoring'
            },
            {
                'url': 'https://www.fda.gov/food/compliance-enforcement-food/food-compliance-programs#cosmetics',
                'category': 'food_cosmetics'
            },
            {
                'url': 'https://www.fda.gov/medical-devices/quality-and-compliance-medical-devices/center-devices-and-radiological-health-cdrh-compliance-programs',
                'category': 'medical_devices'
            },
            {
                'url': 'https://www.fda.gov/drugs/guidance-compliance-regulatory-information/drug-compliance-programs',
                'category': 'drugs'
            },
            {
                'url': 'https://www.fda.gov/food/compliance-enforcement-food/food-compliance-programs',
                'category': 'food'
            },
            {
                'url': 'https://www.fda.gov/animal-veterinary/compliance-enforcement/cvm-compliance-programs',
                'category': 'animal_veterinary'
            }
        ]
    
    def extract_filename_from_response(self, response, pdf_url):
        """Extract filename from Content-Disposition header or URL"""
        # Try to get filename from Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition', '')
        if content_disposition:
            # Parse filename from Content-Disposition
            filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', content_disposition)
            if filename_match:
                filename = filename_match.group(1).strip('\'"')
                return unquote(filename)
        
        # Extract media ID from URL as fallback
        media_id_match = re.search(r'/media/(\d+)/', pdf_url)
        if media_id_match:
            return f"FDA_Media_{media_id_match.group(1)}.pdf"
        
        return None
    
    def download_pdf_with_requests(self, pdf_url: str, category: str, pdf_name: str) -> bool:
        """Download PDF using requests library with proper filename extraction"""
        try:
            # Create category subdirectory
            category_dir = self.output_dir / category
            category_dir.mkdir(exist_ok=True)
            
            logger.info(f"Downloading: {pdf_url}")
            
            # Download the PDF with HEAD request first to get headers
            head_response = self.session.head(pdf_url, timeout=30, allow_redirects=True)
            
            # Extract actual filename from response
            actual_filename = self.extract_filename_from_response(head_response, pdf_url)
            
            # Use the extracted filename or construct one from the title
            if actual_filename:
                safe_filename = actual_filename
            else:
                # Sanitize the provided name
                safe_filename = re.sub(r'[^\w\-_\. ]', '_', pdf_name)
                if not safe_filename.endswith('.pdf'):
                    safe_filename += '.pdf'
                # If name is too generic, add media ID
                if safe_filename in ['PDF.pdf', 'document.pdf']:
                    media_id_match = re.search(r'/media/(\d+)/', pdf_url)
                    if media_id_match:
                        safe_filename = f"FDA_Media_{media_id_match.group(1)}.pdf"
            
            file_path = category_dir / safe_filename
            
            # Skip if already downloaded
            if file_path.exists():
                logger.info(f"✓ Already exists: {safe_filename}")
                return True
            
            # Download the PDF
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if it's actually a PDF
            content_type = response.headers.get('Content-Type', '')
            if 'pdf' not in content_type.lower():
                logger.warning(f"Warning: Content-Type is {content_type}, expected PDF")
            
            # Write to file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = file_path.stat().st_size / 1024  # KB
            logger.info(f"✓ Saved: {safe_filename} ({file_size:.1f} KB)")
            return True
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Error downloading {pdf_url}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error downloading {pdf_url}: {str(e)}")
            return False
    
    async def extract_pdf_links(self, page, url: str) -> List[Dict[str, str]]:
        """Extract all PDF download links with proper titles from a page"""
        try:
            logger.info(f"Loading page: {url}")
            await page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            await page.wait_for_load_state('domcontentloaded')
            await asyncio.sleep(2)
            
            # Execute JavaScript to extract PDF links with their context
            pdf_links_data = await page.evaluate("""
                () => {
                    const links = [];
                    const pdfLinks = document.querySelectorAll('a[href*="/media/"][href*="/download"]');
                    
                    pdfLinks.forEach(link => {
                        const href = link.href;
                        let title = '';
                        
                        // Try to get title from various sources
                        // 1. Check link's own text
                        if (link.textContent.trim() && link.textContent.trim() !== 'PDF') {
                            title = link.textContent.trim();
                        }
                        
                        // 2. Check parent element's text (excluding the link itself)
                        if (!title || title === 'PDF') {
                            const parent = link.parentElement;
                            if (parent) {
                                const parentText = parent.textContent.replace(link.textContent, '').trim();
                                if (parentText && parentText !== 'PDF') {
                                    title = parentText;
                                }
                            }
                        }
                        
                        // 3. Check previous sibling text
                        if (!title || title === 'PDF') {
                            let prevSibling = link.previousSibling;
                            while (prevSibling) {
                                if (prevSibling.nodeType === Node.TEXT_NODE && prevSibling.textContent.trim()) {
                                    title = prevSibling.textContent.trim();
                                    break;
                                } else if (prevSibling.nodeType === Node.ELEMENT_NODE) {
                                    title = prevSibling.textContent.trim();
                                    break;
                                }
                                prevSibling = prevSibling.previousSibling;
                            }
                        }
                        
                        // 4. Check heading in the same section
                        if (!title || title === 'PDF') {
                            const section = link.closest('li, div, section, article');
                            if (section) {
                                const heading = section.querySelector('h1, h2, h3, h4, h5, h6, strong, b');
                                if (heading) {
                                    title = heading.textContent.trim();
                                }
                            }
                        }
                        
                        // 5. Extract media ID as last resort
                        if (!title || title === 'PDF') {
                            const mediaMatch = href.match(/\\/media\\/(\\d+)\\//);
                            if (mediaMatch) {
                                title = `Document_${mediaMatch[1]}`;
                            }
                        }
                        
                        links.push({
                            url: href,
                            title: title
                        });
                    });
                    
                    return links;
                }
            """)
            
            pdf_links = []
            for link_data in pdf_links_data:
                # Clean up title
                title = re.sub(r'\s+', ' ', link_data['title']).strip()
                title = title[:200]  # Limit length
                
                pdf_links.append({
                    'url': link_data['url'],
                    'name': title
                })
                logger.info(f"  Found: {title}")
            
            return pdf_links
            
        except Exception as e:
            logger.error(f"✗ Error extracting links from {url}: {str(e)}")
            return []
    
    async def process_page(self, browser, page_info: Dict[str, str]):
        """Process a single FDA compliance page"""
        url = page_info['url']
        category = page_info['category']
        
        logger.info(f"\n{'='*70}")
        logger.info(f"Processing: {category}")
        logger.info(f"URL: {url}")
        logger.info(f"{'='*70}")
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        try:
            # Create page for extracting links
            page = await context.new_page()
            
            # Extract PDF links using Playwright
            pdf_links = await self.extract_pdf_links(page, url)
            logger.info(f"\n✓ Found {len(pdf_links)} PDFs on {category}\n")
            
            # Close the extraction page
            await page.close()
            
            # Download each PDF using requests
            download_count = 0
            failed_count = 0
            
            for i, pdf_info in enumerate(pdf_links, 1):
                logger.info(f"[{i}/{len(pdf_links)}] {pdf_info['name']}")
                
                success = self.download_pdf_with_requests(
                    pdf_info['url'],
                    category,
                    pdf_info['name']
                )
                
                if success:
                    download_count += 1
                else:
                    failed_count += 1
                
                # Be respectful - add delay between downloads
                await asyncio.sleep(1)
            
            logger.info(f"\n{'='*70}")
            logger.info(f"Category: {category}")
            logger.info(f"✓ Successfully downloaded: {download_count}/{len(pdf_links)}")
            if failed_count > 0:
                logger.info(f"✗ Failed: {failed_count}")
            logger.info(f"{'='*70}\n")
            
        except Exception as e:
            logger.error(f"✗ Error processing {category}: {str(e)}")
        
        finally:
            await context.close()
    
    async def run(self):
        """Main execution method"""
        logger.info("="*70)
        logger.info("FDA Compliance PDF Downloader")
        logger.info("="*70)
        logger.info(f"Output directory: {self.output_dir.absolute()}\n")
        
        async with async_playwright() as p:
            # Launch browser in headless mode
            browser = await p.chromium.launch(headless=True)
            
            try:
                # Process each page
                for page_info in self.urls:
                    await self.process_page(browser, page_info)
                
                logger.info("\n" + "="*70)
                logger.info("✓ ALL DOWNLOADS COMPLETE!")
                logger.info(f"Files saved to: {self.output_dir.absolute()}")
                logger.info("="*70)
                
            finally:
                await browser.close()
                self.session.close()


async def main():
    """Entry point"""
    downloader = FDACompliancePDFDownloader()
    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())