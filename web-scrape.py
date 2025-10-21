#!/usr/bin/env python3
"""
Web Scraper with PDF Generation - FIXED DOWNLOAD DIRECTORY
Downloads files to the script directory (nih folder)
"""

import os
import sys
import time
from urllib.parse import urljoin, urlparse, urlunparse
from collections import deque
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_JUSTIFY
import re


class WebScraperPDF:
    def __init__(self, base_url, max_depth, output_name):
        self.base_url = base_url
        self.max_depth = max_depth
        self.output_name = output_name
        self.visited_urls = set()
        self.scraped_content = []
        self.error_urls = set()
        self.skipped_urls = set()
        self.domain = urlparse(base_url).netloc
        
        # URL patterns to exclude
        self.exclude_patterns = [
            r'/export/',
            r'/print/',
            r'/download/',
            r'\.pdf$',
            r'\.csv$',
            r'\.xlsx?$',
            r'\.docx?$',
            r'\?print=',
            r'\?export=',
        ]
        
        # Get script directory (the "nih" folder)
        if getattr(sys, 'frozen', False):
            self.script_dir = os.path.dirname(sys.executable)
        else:
            self.script_dir = os.path.dirname(os.path.abspath(__file__))
        
        if not self.script_dir:
            self.script_dir = os.getcwd()
        
        # Ensure the directory exists
        os.makedirs(self.script_dir, exist_ok=True)
        
        print(f"\n{'='*80}")
        print(f"WORKING DIRECTORY: {self.script_dir}")
        print(f"PDF FILES WILL BE SAVED TO: {self.script_dir}")
        print(f"DOWNLOADS WILL BE SAVED TO: {self.script_dir}")
        print(f"{'='*80}\n")
        
        # Setup Selenium with custom download directory
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        # CRITICAL: Set download directory to script directory
        prefs = {
            "download.default_directory": self.script_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True  # Download PDFs instead of opening
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
            # Verify download directory is set correctly
            print(f"✅ Chrome download directory configured to: {self.script_dir}\n")
            
        except Exception as e:
            print(f"❌ Error initializing Chrome driver: {e}")
            sys.exit(1)
    
    def should_exclude_url(self, url):
        """Check if URL should be excluded"""
        for pattern in self.exclude_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        return False
    
    def normalize_url(self, url):
        """Normalize URL"""
        parsed = urlparse(url)
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            ''
        ))
        return normalized.rstrip('/')
    
    def is_valid_url(self, url):
        """Check if URL is valid for scraping"""
        parsed = urlparse(url)
        
        if parsed.netloc != self.domain:
            return False
        
        if self.should_exclude_url(url):
            return False
        
        return True
    
    def is_error_page(self, soup, url):
        """Detect if page is an actual error page"""
        # Check if redirected to explicit error path
        if '/Error/Index' in url or '/error/index' in url.lower():
            return True
        
        # Check title for error indicators
        title = soup.find('title')
        if title:
            title_text = title.get_text().lower()
            if any(indicator in title_text for indicator in [
                'server error',
                'page not found',
                '404 error',
                '500 error',
                'error occurred'
            ]):
                return True
        
        # Check for specific error page structure
        error_headers = soup.find_all(['h1', 'h2'])
        for header in error_headers:
            header_text = header.get_text().strip().lower()
            if header_text in ['server error', 'an error occurred while processing your request']:
                return True
        
        return False
    
    def expand_accordions(self):
        """Expand all accordion elements"""
        try:
            accordion_selectors = [
                "button[aria-expanded='false']",
                ".accordion-button.collapsed",
                "[data-toggle='collapse']",
                "[data-bs-toggle='collapse']",
                ".collapse:not(.show)"
            ]
            
            for selector in accordion_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(0.2)
                            self.driver.execute_script("arguments[0].click();", element)
                            time.sleep(0.3)
                        except:
                            continue
                except:
                    continue
            
            time.sleep(1)
        except Exception as e:
            print(f"⚠️  Warning: Could not expand accordions: {e}")
    
    def extract_body_text(self, url):
        """Extract body text with improved error handling"""
        try:
            self.driver.get(url)
            time.sleep(3)
            
            # Check current URL for error redirect
            current_url = self.driver.current_url
            if '/Error/Index' in current_url:
                print(f"    ⚠️  Redirected to error page")
                self.error_urls.add(url)
                return None
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Check if this is an actual error page
            if self.is_error_page(soup, current_url):
                print(f"    ⚠️  Error page detected")
                self.error_urls.add(url)
                return None
            
            self.expand_accordions()
            
            # Re-parse after accordion expansion
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(["script", "style", "nav", "header", "footer", "aside", "iframe", "noscript"]):
                element.decompose()
            
            body = soup.find('body')
            if body:
                text = body.get_text(separator='\n', strip=True)
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                content = '\n\n'.join(lines)
                
                # Minimum content check
                if len(content) < 50:
                    print(f"    ⚠️  Content too short ({len(content)} chars)")
                    self.error_urls.add(url)
                    return None
                
                return content
            return None
            
        except TimeoutException:
            print(f"    ❌ Timeout")
            self.error_urls.add(url)
            return None
        except WebDriverException as e:
            print(f"    ❌ WebDriver error: {str(e)[:50]}...")
            self.error_urls.add(url)
            return None
        except Exception as e:
            print(f"    ❌ Error: {str(e)[:50]}...")
            self.error_urls.add(url)
            return None
    
    def get_links(self, url):
        """Extract valid links"""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            links = set()
            
            for link in soup.find_all('a', href=True):
                absolute_url = urljoin(url, link['href'])
                normalized_url = self.normalize_url(absolute_url)
                
                if self.is_valid_url(normalized_url):
                    links.add(normalized_url)
                elif self.should_exclude_url(normalized_url):
                    self.skipped_urls.add(normalized_url)
            
            return list(links)
        except Exception as e:
            print(f"    ❌ Error getting links: {e}")
            return []
    
    def scrape(self):
        """Perform breadth-first scraping"""
        queue = deque([(self.base_url, 0)])
        
        print(f"\n🌐 Starting scrape: {self.base_url}")
        print(f"📊 Maximum depth: {self.max_depth}")
        print(f"⏱️  Rate limit: 2.5 seconds between requests\n")
        
        page_count = 0
        success_count = 0
        
        while queue:
            url, depth = queue.popleft()
            
            if url in self.visited_urls or url in self.error_urls or depth > self.max_depth:
                continue
            
            page_count += 1
            print(f"[{page_count}] [Depth {depth}] {url}")
            self.visited_urls.add(url)
            
            content = self.extract_body_text(url)
            
            if content:
                # Check for duplicate content
                is_duplicate = False
                for existing in self.scraped_content:
                    if existing['content'] == content:
                        print(f"    ⚠️  Duplicate content, skipping")
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    self.scraped_content.append({
                        'url': url,
                        'depth': depth,
                        'content': content,
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'char_count': len(content)
                    })
                    success_count += 1
                    print(f"    ✅ {len(content):,} characters extracted")
            else:
                print(f"    ❌ Failed to extract content")
            
            # Get links for next depth
            if depth < self.max_depth and content:
                links = self.get_links(url)
                new_links = [l for l in links if l not in self.visited_urls and l not in self.error_urls]
                if new_links:
                    print(f"    → Found {len(new_links)} new links")
                    for link in new_links:
                        queue.append((link, depth + 1))
            
            time.sleep(2.5)
        
        self.driver.quit()
        
        print(f"\n{'='*80}")
        print(f"✅ Scraping complete!")
        print(f"   Pages visited: {len(self.visited_urls)}")
        print(f"   Successful extractions: {success_count}")
        print(f"   Errors encountered: {len(self.error_urls)}")
        print(f"   URLs skipped (export/print): {len(self.skipped_urls)}")
        print(f"{'='*80}\n")
    
    def generate_pdfs(self):
        """Generate PDFs in 4.5MB segments"""
        if not self.scraped_content:
            print("❌ No content to generate PDFs")
            return
        
        MAX_SIZE_BYTES = int(4.5 * 1024 * 1024)
        
        print(f"📄 Generating PDFs (max {MAX_SIZE_BYTES / (1024*1024):.1f}MB each)...\n")
        
        file_number = 1
        current_batch = []
        current_size_estimate = 0
        
        for item in self.scraped_content:
            item_size = len(item['content']) * 3
            
            if current_size_estimate + item_size > MAX_SIZE_BYTES and current_batch:
                self.create_pdf(current_batch, file_number)
                file_number += 1
                current_batch = []
                current_size_estimate = 0
            
            current_batch.append(item)
            current_size_estimate += item_size
        
        if current_batch:
            self.create_pdf(current_batch, file_number)
        
        print(f"\n{'='*80}")
        print(f"✅ PDF generation complete! Created {file_number} PDF file(s)")
        print(f"   All files saved to: {self.script_dir}")
        print(f"{'='*80}")
    
    def create_pdf(self, content_list, file_number):
        """Create a single PDF file"""
        filename = f"{self.output_name}_part{file_number:03d}.pdf"
        full_path = os.path.join(self.script_dir, filename)
        
        version = 1
        while os.path.exists(full_path):
            filename = f"{self.output_name}_part{file_number:03d}_v{version}.pdf"
            full_path = os.path.join(self.script_dir, filename)
            version += 1
        
        print(f"📝 Creating: {filename}")
        
        try:
            doc = SimpleDocTemplate(
                full_path,
                pagesize=letter,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=0.75*inch,
                bottomMargin=0.75*inch
            )
            
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(
                name='CustomBody',
                parent=styles['BodyText'],
                alignment=TA_JUSTIFY,
                fontSize=10,
                leading=14,
                spaceAfter=6
            ))
            
            story = []
            
            # Title page
            story.append(Paragraph(f"<b>{self.output_name}</b>", styles['Title']))
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph(f"<b>Part {file_number}</b>", styles['Heading1']))
            story.append(Spacer(1, 0.3*inch))
            story.append(Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
            story.append(Paragraph(f"<b>Source:</b> {self.base_url}", styles['Normal']))
            story.append(Paragraph(f"<b>Pages in document:</b> {len(content_list)}", styles['Normal']))
            story.append(Paragraph(f"<b>Max depth scraped:</b> {self.max_depth}", styles['Normal']))
            story.append(PageBreak())
            
            # Content pages
            for idx, item in enumerate(content_list, 1):
                # Page header
                story.append(Paragraph(f"<b>Page {idx} of {len(content_list)}</b>", styles['Heading1']))
                story.append(Spacer(1, 0.1*inch))
                story.append(Paragraph(f"<b>URL:</b> {item['url']}", styles['Heading2']))
                story.append(Paragraph(
                    f"<b>Depth:</b> {item['depth']} | <b>Scraped:</b> {item['timestamp']} | <b>Size:</b> {item['char_count']:,} chars",
                    styles['Normal']
                ))
                story.append(Spacer(1, 0.2*inch))
                
                # Content paragraphs
                paragraphs = item['content'].split('\n\n')
                for para in paragraphs:
                    if para.strip():
                        # Escape XML special characters for ReportLab
                        safe_para = (para.replace('&', '&amp;')
                                        .replace('<', '&lt;')
                                        .replace('>', '&gt;'))
                        
                        try:
                            story.append(Paragraph(safe_para, styles['CustomBody']))
                        except Exception as para_error:
                            # If paragraph fails to render, add as plain text notice
                            story.append(Paragraph(
                                f"[Content formatting error: {str(para_error)[:50]}]", 
                                styles['Normal']
                            ))
                
                story.append(PageBreak())
            
            # Build the PDF
            doc.build(story)
            
            # Verify file was created and get actual size
            if os.path.exists(full_path):
                file_size = os.path.getsize(full_path)
                file_size_mb = file_size / (1024 * 1024)
                print(f"    ✅ Success: {file_size_mb:.2f} MB ({len(content_list)} pages)")
                print(f"       Location: {full_path}")
            else:
                print(f"    ❌ ERROR: File was not created at {full_path}")
                
        except Exception as e:
            print(f"    ❌ Error creating PDF: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main CLI interface"""
    print("\n" + "="*80)
    print(" "*25 + "WEB SCRAPER → PDF")
    print("="*80)
    
    # Get URL
    url = input("\n📍 Enter URL to scrape: ").strip()
    if not url.startswith(('http://', 'https://')):
        print("❌ URL must start with http:// or https://")
        sys.exit(1)
    
    # Get depth
    while True:
        try:
            depth_input = input("\n📊 Enter scrape depth (1-5): ").strip()
            depth = int(depth_input)
            if 1 <= depth <= 5:
                break
            print("❌ Depth must be 1-5")
        except ValueError:
            print("❌ Enter a valid number")
    
    # Get filename
    output_name = input("\n📝 Enter output filename (no extension): ").strip()
    if not output_name:
        output_name = "scraped_content"
    
    # Remove extension if provided
    output_name = os.path.splitext(output_name)[0]
    
    # Confirm settings
    print("\n" + "="*80)
    print("CONFIGURATION:")
    print(f"  URL:      {url}")
    print(f"  Depth:    {depth}")
    print(f"  Output:   {output_name}_part001.pdf, {output_name}_part002.pdf, ...")
    print(f"  Max Size: 4.5 MB per PDF")
    print(f"  Format:   PDF ONLY (no CSV)")
    print(f"  Rate:     2.5 seconds between requests (respectful scraping)")
    print(f"  Downloads: All files saved to script directory")
    print("="*80)
    
    confirm = input("\n▶️  Proceed? (y/n): ").strip().lower()
    if confirm != 'y':
        print("\n❌ Cancelled by user")
        sys.exit(0)
    
    # Execute scraping
    try:
        print("\n" + "="*80)
        print("STARTING SCRAPE...")
        print("="*80)
        
        scraper = WebScraperPDF(url, depth, output_name)
        scraper.scrape()
        scraper.generate_pdfs()
        
        print("\n" + "="*80)
        print("✅ COMPLETE! All files saved to script directory.")
        print(f"   Directory: {scraper.script_dir}")
        print(f"   PDFs: {output_name}_part001.pdf, etc.")
        print(f"   Downloads: Any downloaded files are also in this directory")
        print("="*80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n❌ Process interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()            