import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin, urlparse, unquote
import re
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class HealthMilDocumentDownloader:
    def __init__(self, output_folder="healthmil_documents"):
        self.base_url = "https://www.health.mil"
        self.target_pages = [
            "https://www.health.mil/Reference-Center/Technical-Documents",
            "https://www.health.mil/Reference-Center/Reports",
            "https://www.health.mil/Reference-Center/Publications",
            "https://www.health.mil/Reference-Center/Presentations",
            "https://www.health.mil/Reference-Center/Fact-Sheets",
            "https://www.health.mil/Reference-Center/Congressional-Testimonies",
            "https://www.health.mil/Reference-Center/Meeting-References"
        ]
        
        self.output_folder = output_folder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Disable SSL verification
        self.session.verify = False
        
        # Create single output folder
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            print(f"Created output folder: {self.output_folder}")
        
        # Statistics
        self.stats = {
            'total_doc_pages': 0,
            'downloaded': 0,
            'skipped': 0,
            'errors': 0,
            'by_type': {}
        }
    
    def sanitize_filename(self, filename):
        """Remove invalid characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        # Remove extra spaces and limit length
        filename = ' '.join(filename.split())
        if len(filename) > 150:
            filename = filename[:150]
        return filename
    
    def get_extension_from_content_type(self, content_type):
        """Determine file extension from Content-Type header"""
        content_type = content_type.lower()
        
        if 'pdf' in content_type:
            return '.pdf'
        elif 'powerpoint' in content_type or 'presentation' in content_type:
            if 'openxmlformats' in content_type:
                return '.pptx'
            return '.ppt'
        elif 'word' in content_type or 'msword' in content_type:
            if 'openxmlformats' in content_type:
                return '.docx'
            return '.doc'
        elif 'excel' in content_type or 'spreadsheet' in content_type:
            if 'openxmlformats' in content_type:
                return '.xlsx'
            return '.xls'
        elif 'text/plain' in content_type:
            return '.txt'
        elif 'text/csv' in content_type:
            return '.csv'
        else:
            return '.pdf'  # Default to PDF
    
    def get_document_page_links(self, page_url):
        """Extract all document page links from a listing page"""
        print(f"\n{'='*60}")
        print(f"Scanning: {page_url}")
        print(f"{'='*60}")
        
        try:
            response = self.session.get(page_url, timeout=30, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            document_page_links = []
            
            # Look for links that match the pattern of document pages
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(self.base_url, href)
                
                # Check if this looks like a document page link
                # Pattern: /Reference-Center/[Category]/YYYY/MM/DD/[Document-Name]
                if re.search(r'/Reference-Center/[^/]+/\d{4}/\d{2}/\d{2}/', full_url):
                    link_text = link.get_text(strip=True)
                    if link_text and len(link_text) > 3:
                        document_page_links.append({
                            'url': full_url,
                            'title': link_text
                        })
                        print(f"  ✓ Found document page: {link_text[:60]}...")
            
            print(f"\nTotal document pages found: {len(document_page_links)}")
            return document_page_links
            
        except Exception as e:
            print(f"  ✗ Error scanning page: {e}")
            return []
    
    def try_direct_download(self, doc_page_info):
        """Try downloading directly from the document page URL"""
        try:
            doc_url = doc_page_info['url']
            doc_title = doc_page_info['title']
            
            print(f"  Attempting direct download from: {doc_url}")
            
            # Make a HEAD request first to check Content-Type
            head_response = self.session.head(doc_url, timeout=30, verify=False, allow_redirects=True)
            content_type = head_response.headers.get('Content-Type', '').lower()
            
            # Check if it's a downloadable file (not HTML)
            if 'html' not in content_type and any(x in content_type for x in ['pdf', 'word', 'excel', 'powerpoint', 'document', 'application']):
                print(f"  ✓ Direct download detected (Content-Type: {content_type})")
                
                # Determine extension from Content-Type
                extension = self.get_extension_from_content_type(content_type)
                
                # Download the file
                return self.download_file_direct(doc_url, doc_title, extension)
            else:
                print(f"  ⚠ Not a direct download (Content-Type: {content_type}), searching page...")
                return False
                
        except Exception as e:
            print(f"  ⚠ Direct download failed: {e}")
            return False
    
    def download_file_direct(self, file_url, title, extension):
        """Download file directly to the single output folder"""
        try:
            # Create filename
            safe_title = self.sanitize_filename(title)
            filename = f"{safe_title}{extension}"
            filepath = os.path.join(self.output_folder, filename)
            
            # Handle duplicates
            counter = 1
            original_filepath = filepath
            while os.path.exists(filepath):
                name, ext = os.path.splitext(original_filepath)
                filepath = f"{name}_{counter}{ext}"
                counter += 1
            
            # Check if already exists
            if os.path.exists(original_filepath) and counter == 1:
                print(f"  ⊘ Skipping (exists): {filename}")
                self.stats['skipped'] += 1
                return True
            
            print(f"  ⬇ Downloading: {filename}")
            
            # Download file
            response = self.session.get(file_url, timeout=120, stream=True, verify=False)
            response.raise_for_status()
            
            # Write file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Update stats
            self.stats['downloaded'] += 1
            ext_upper = extension.upper()
            self.stats['by_type'][ext_upper] = self.stats['by_type'].get(ext_upper, 0) + 1
            
            print(f"  ✓ Downloaded: {filename}")
            return True
            
        except Exception as e:
            print(f"  ✗ Error downloading: {e}")
            self.stats['errors'] += 1
            return False
    
    def find_download_link_on_page(self, doc_page_info):
        """Fallback: Search for download links on the page"""
        try:
            doc_url = doc_page_info['url']
            doc_title = doc_page_info['title']
            
            response = self.session.get(doc_url, timeout=30, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for download links
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Check if it's a file link
                if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx']):
                    download_url = urljoin(self.base_url, href)
                    extension = os.path.splitext(urlparse(download_url).path)[1]
                    return self.download_file_direct(download_url, doc_title, extension)
            
            # Check iframes/embeds
            for tag in soup.find_all(['iframe', 'embed', 'object']):
                src = tag.get('src') or tag.get('data')
                if src and any(ext in src.lower() for ext in ['.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx']):
                    download_url = urljoin(self.base_url, src)
                    extension = os.path.splitext(urlparse(download_url).path)[1]
                    return self.download_file_direct(download_url, doc_title, extension)
            
            print(f"  ⚠ No download link found on page")
            self.stats['errors'] += 1
            return False
            
        except Exception as e:
            print(f"  ✗ Error searching page: {e}")
            self.stats['errors'] += 1
            return False
    
    def process_document_page(self, doc_page_info):
        """Process a document page - try direct download first, then search"""
        print(f"\n  Processing: {doc_page_info['title'][:50]}...")
        
        # Strategy 1: Try direct download from the page URL
        if self.try_direct_download(doc_page_info):
            return True
        
        # Strategy 2: If direct download fails, search the page for links
        return self.find_download_link_on_page(doc_page_info)
    
    def run(self):
        """Main execution method"""
        print("=" * 70)
        print("Health.mil Reference Center Document Downloader")
        print("SSL Certificate Verification: DISABLED (DoD/Army Certs)")
        print("=" * 70)
        print(f"\nTarget pages: {len(self.target_pages)}")
        print(f"All documents will be saved to: {self.output_folder}")
        print("Strategy: Direct download from document page URLs\n")
        
        # Process each listing page
        for page_num, page_url in enumerate(self.target_pages, 1):
            print(f"\n[Page {page_num}/{len(self.target_pages)}]")
            
            # Get all document page links
            doc_page_links = self.get_document_page_links(page_url)
            self.stats['total_doc_pages'] += len(doc_page_links)
            
            # Process each document page
            for doc_num, doc_page_info in enumerate(doc_page_links, 1):
                print(f"\n  [{doc_num}/{len(doc_page_links)}]", end=" ")
                self.process_document_page(doc_page_info)
                
                # Be respectful - delay between requests
                time.sleep(2)
            
            # Delay between listing pages
            time.sleep(3)
        
        # Print final summary
        self.print_summary()
    
    def print_summary(self):
        """Print download statistics"""
        print("\n" + "=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)
        print(f"Total document pages found: {self.stats['total_doc_pages']}")
        print(f"Successfully downloaded:     {self.stats['downloaded']}")
        print(f"Skipped (already exists):    {self.stats['skipped']}")
        print(f"Errors:                      {self.stats['errors']}")
        print(f"\nAll documents saved to: {os.path.abspath(self.output_folder)}")
        
        if self.stats['by_type']:
            print("\nDocuments by type:")
            for doc_type, count in sorted(self.stats['by_type'].items()):
                print(f"  {doc_type}: {count}")
        
        print("=" * 70)

# Run the downloader
if __name__ == "__main__":
    downloader = HealthMilDocumentDownloader(output_folder="healthmil_documents")
    downloader.run()