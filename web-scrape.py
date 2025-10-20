#!/usr/bin/env python3
"""
Web Scraper with PDF Generation
Scrapes websites to specified depth and generates PDFs in 4.5MB segments
"""

import os
import sys
import time
from urllib.parse import urljoin, urlparse
from collections import deque
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_JUSTIFY
import io


class WebScraper:
    def __init__(self, base_url, max_depth, output_name):
        self.base_url = base_url
        self.max_depth = max_depth
        self.output_name = output_name
        self.visited_urls = set()
        self.scraped_content = []
        self.domain = urlparse(base_url).netloc
        
        # Setup Selenium for JavaScript content
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=chrome_options)
        
    def is_valid_url(self, url):
        """Check if URL belongs to the same domain"""
        parsed = urlparse(url)
        return parsed.netloc == self.domain
    
    def expand_accordions(self):
        """Expand all accordion elements on the page"""
        try:
            # Common accordion selectors
            accordion_selectors = [
                "button[aria-expanded='false']",
                ".accordion-button.collapsed",
                "[data-toggle='collapse']",
                ".collapse:not(.show)"
            ]
            
            for selector in accordion_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        element.click()
                        time.sleep(0.3)  # Wait for expansion
                    except:
                        continue
        except Exception as e:
            print(f"Warning: Could not expand all accordions: {e}")
    
    def extract_body_text(self, url):
        """Extract body text from URL, including accordion content"""
        try:
            self.driver.get(url)
            time.sleep(2)  # Wait for page load
            
            # Expand accordions
            self.expand_accordions()
            
            # Get page source after JavaScript execution
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "header", "footer"]):
                script.decompose()
            
            # Extract body text
            body = soup.find('body')
            if body:
                text = body.get_text(separator='\n', strip=True)
                # Clean up excessive whitespace
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                return '\n'.join(lines)
            return ""
        except Exception as e:
            print(f"Error extracting text from {url}: {e}")
            return ""
    
    def get_links(self, url):
        """Extract all links from the page"""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            links = []
            for link in soup.find_all('a', href=True):
                absolute_url = urljoin(url, link['href'])
                if self.is_valid_url(absolute_url):
                    links.append(absolute_url)
            return links
        except Exception as e:
            print(f"Error getting links from {url}: {e}")
            return []
    
    def scrape(self):
        """Perform breadth-first scraping to specified depth"""
        queue = deque([(self.base_url, 0)])  # (url, depth)
        
        while queue:
            url, depth = queue.popleft()
            
            if url in self.visited_urls or depth > self.max_depth:
                continue
            
            print(f"Scraping (depth {depth}): {url}")
            self.visited_urls.add(url)
            
            # Extract content
            content = self.extract_body_text(url)
            if content:
                self.scraped_content.append({
                    'url': url,
                    'depth': depth,
                    'content': content
                })
            
            # Get links for next depth level
            if depth < self.max_depth:
                links = self.get_links(url)
                for link in links:
                    if link not in self.visited_urls:
                        queue.append((link, depth + 1))
            
            time.sleep(1)  # Be respectful with rate limiting
        
        self.driver.quit()
    
    def generate_pdfs(self):
        """Generate PDFs in 4.5MB segments"""
        max_size = 4.5 * 1024 * 1024  # 4.5MB in bytes
        file_counter = 1
        current_content = []
        
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name='Justify',
            parent=styles['BodyText'],
            alignment=TA_JUSTIFY,
            fontSize=10,
            leading=14
        ))
        
        for item in self.scraped_content:
            current_content.append(item)
            
            # Check if we need to create a PDF
            if self.estimate_pdf_size(current_content) >= max_size:
                self.create_pdf(current_content[:-1], file_counter, styles)
                file_counter += 1
                current_content = [item]  # Start new PDF with current item
        
        # Create final PDF with remaining content
        if current_content:
            self.create_pdf(current_content, file_counter, styles)
    
    def estimate_pdf_size(self, content_list):
        """Estimate PDF size (rough approximation)"""
        total_chars = sum(len(item['content']) for item in content_list)
        # Rough estimate: 1 character ≈ 2 bytes in PDF
        return total_chars * 2
    
    def create_pdf(self, content_list, file_number, styles):
        """Create a single PDF file"""
        filename = f"{self.output_name}_{file_number}.pdf"
        
        doc = SimpleDocTemplate(
            filename,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        
        story = []
        
        for item in content_list:
            # Add URL as heading
            story.append(Paragraph(f"<b>Source: {item['url']}</b>", styles['Heading2']))
            story.append(Spacer(1, 0.2*inch))
            
            # Add content
            # Split content into paragraphs
            paragraphs = item['content'].split('\n\n')
            for para in paragraphs:
                if para.strip():
                    # Escape special characters for reportlab
                    safe_para = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    story.append(Paragraph(safe_para, styles['Justify']))
                    story.append(Spacer(1, 0.1*inch))
            
            story.append(PageBreak())
        
        doc.build(story)
        print(f"Created: {filename}")


def main():
    """Main CLI interface"""
    print("=" * 60)
    print("Web Scraper with PDF Generation")
    print("=" * 60)
    
    # Get URL
    url = input("\nEnter the URL to scrape (e.g., https://policymanual.nih.gov/): ").strip()
    if not url.startswith(('http://', 'https://')):
        print("Error: URL must start with http:// or https://")
        sys.exit(1)
    
    # Get depth
    while True:
        try:
            depth = int(input("\nEnter scrape depth (1-5): ").strip())
            if 1 <= depth <= 5:
                break
            else:
                print("Depth must be between 1 and 5")
        except ValueError:
            print("Please enter a valid number")
    
    # Get output filename
    output_name = input("\nEnter output filename (without extension): ").strip()
    if not output_name:
        output_name = "scraped_content"
    
    # Confirm settings
    print("\n" + "=" * 60)
    print("Settings:")
    print(f"  URL: {url}")
    print(f"  Depth: {depth}")
    print(f"  Output: {output_name}_[n].pdf")
    print("=" * 60)
    
    confirm = input("\nProceed? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled.")
        sys.exit(0)
    
    # Start scraping
    print("\nStarting scrape...\n")
    scraper = WebScraper(url, depth, output_name)
    scraper.scrape()
    
    print(f"\nScraped {len(scraper.visited_urls)} pages")
    print("Generating PDFs...\n")
    scraper.generate_pdfs()
    
    print("\nComplete!")


if __name__ == "__main__":
    main()
