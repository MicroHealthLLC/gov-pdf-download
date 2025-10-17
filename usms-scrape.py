import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

# Create output directory
OUTPUT_DIR = "usmarshals_pdfs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# List of URLs to scrape
URLS = [
    "https://www.usmarshals.gov/resources/publications",
    "https://www.usmarshals.gov/resources/publications/policy-directives",
    "https://www.usmarshals.gov/resources/fact-sheets",
    "https://www.usmarshals.gov/resources/guideline"
]

def download_pdf(pdf_url, output_dir):
    """Download a PDF file from the given URL"""
    try:
        # Extract filename from URL
        filename = os.path.basename(urlparse(pdf_url).path)
        
        # Skip if already downloaded
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            print(f"Already exists: {filename}")
            return
        
        # Download the PDF
        print(f"Downloading: {filename}")
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()
        
        # Save the PDF
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"Saved: {filename}")
        
    except Exception as e:
        print(f"Error downloading {pdf_url}: {str(e)}")

def scrape_pdfs_from_page(url):
    """Scrape all PDF links from a given page"""
    try:
        print(f"\nScraping: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Check if it's a PDF link
            if href.endswith('.pdf'):
                # Convert relative URLs to absolute
                full_url = urljoin(url, href)
                pdf_links.append(full_url)
        
        print(f"Found {len(pdf_links)} PDF(s)")
        return pdf_links
        
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return []

def main():
    """Main function to scrape all URLs and download PDFs"""
    print("US Marshals PDF Scraper")
    print("=" * 50)
    
    all_pdfs = []
    
    # Scrape each URL
    for url in URLS:
        pdf_links = scrape_pdfs_from_page(url)
        all_pdfs.extend(pdf_links)
        time.sleep(1)  # Be polite to the server
    
    # Remove duplicates
    all_pdfs = list(set(all_pdfs))
    print(f"\nTotal unique PDFs found: {len(all_pdfs)}")
    print("=" * 50)
    
    # Download all PDFs
    for i, pdf_url in enumerate(all_pdfs, 1):
        print(f"\n[{i}/{len(all_pdfs)}]")
        download_pdf(pdf_url, OUTPUT_DIR)
        time.sleep(0.5)  # Be polite to the server
    
    print("\n" + "=" * 50)
    print(f"Download complete! PDFs saved to: {OUTPUT_DIR}")
    print(f"Total files downloaded: {len(os.listdir(OUTPUT_DIR))}")

if __name__ == "__main__":
    main()
