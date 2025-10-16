import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin, urlparse
import re

class CMSManualDownloader:
    def __init__(self, output_folder="cms_manuals_pdfs"):
        self.base_url = "https://www.cms.gov"
        self.main_page = "https://www.cms.gov/medicare/regulations-guidance/manuals/internet-only-manuals-ioms"
        self.output_folder = output_folder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create output folder if it doesn't exist
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            print(f"Created folder: {self.output_folder}")
    
    def get_publication_links(self):
        """Extract all publication page links from the main IOM page"""
        print("Fetching main page...")
        try:
            response = self.session.get(self.main_page, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all publication links (adjust selector based on actual HTML structure)
            publication_links = []
            
            # Look for links in the table or list
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Match publication number patterns like /cms050111, /cms012345, etc.
                if re.search(r'/cms\d+', href) or 'internet-only-manuals-ioms-items' in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in publication_links:
                        publication_links.append(full_url)
                        print(f"Found publication: {full_url}")
            
            print(f"\nTotal publications found: {len(publication_links)}")
            return publication_links
            
        except Exception as e:
            print(f"Error fetching main page: {e}")
            return []
    
    def get_pdf_links(self, publication_url):
        """Extract all PDF download links from a publication page"""
        print(f"\nFetching PDFs from: {publication_url}")
        try:
            response = self.session.get(publication_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            pdf_links = []
            
            # Find all links that contain 'download' or end with .pdf
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'download' in href.lower() or href.lower().endswith('.pdf'):
                    full_url = urljoin(self.base_url, href)
                    if full_url.lower().endswith('.pdf'):
                        pdf_links.append(full_url)
                        print(f"  Found PDF: {os.path.basename(full_url)}")
            
            return pdf_links
            
        except Exception as e:
            print(f"Error fetching publication page {publication_url}: {e}")
            return []
    
    def download_pdf(self, pdf_url):
        """Download a single PDF file"""
        try:
            # Extract filename from URL
            filename = os.path.basename(urlparse(pdf_url).path)
            filepath = os.path.join(self.output_folder, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                print(f"  Skipping (already exists): {filename}")
                return True
            
            print(f"  Downloading: {filename}")
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Write file in chunks
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"  ✓ Downloaded: {filename}")
            return True
            
        except Exception as e:
            print(f"  ✗ Error downloading {pdf_url}: {e}")
            return False
    
    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("CMS Medicare Manuals PDF Downloader")
        print("=" * 60)
        
        # Step 1: Get all publication links
        publication_links = self.get_publication_links()
        
        if not publication_links:
            print("No publications found. Please check the website structure.")
            return
        
        # Step 2: Process each publication
        total_pdfs = 0
        downloaded_pdfs = 0
        
        for i, pub_url in enumerate(publication_links, 1):
            print(f"\n[{i}/{len(publication_links)}] Processing publication...")
            
            # Get PDF links from this publication
            pdf_links = self.get_pdf_links(pub_url)
            total_pdfs += len(pdf_links)
            
            # Download each PDF
            for pdf_url in pdf_links:
                if self.download_pdf(pdf_url):
                    downloaded_pdfs += 1
                
                # Be respectful - add delay between downloads
                time.sleep(1)
            
            # Delay between publications
            time.sleep(2)
        
        # Summary
        print("\n" + "=" * 60)
        print(f"Download Complete!")
        print(f"Total PDFs found: {total_pdfs}")
        print(f"Successfully downloaded: {downloaded_pdfs}")
        print(f"Output folder: {os.path.abspath(self.output_folder)}")
        print("=" * 60)

# Run the scraper
if __name__ == "__main__":
    downloader = CMSManualDownloader(output_folder="cms_manuals_pdfs")
    downloader.run()