import requests
from bs4 import BeautifulSoup
from pathlib import Path
import re
from urllib.parse import urljoin, unquote
import time

# Setup
output_dir = Path("fema_guidance")
output_dir.mkdir(exist_ok=True)

# Get the page
url = "https://www.fema.gov/about/reports-and-data/guidance"
print(f"Fetching page: {url}")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

response = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(response.text, 'html.parser')

# Find all PDF links
pdf_links = []
for link in soup.find_all('a', href=True):
    href = link['href']
    if '.pdf' in href or '/sites/default/files/' in href:
        # Clean up the URL - remove trailing spaces and decode
        full_url = urljoin(url, href)
        full_url = full_url.strip()  # Remove leading/trailing spaces
        full_url = unquote(full_url)  # Decode URL encoding
        full_url = full_url.rstrip()  # Remove any remaining whitespace
        
        title = link.get_text(strip=True) or href.split('/')[-1]
        pdf_links.append({'url': full_url, 'title': title})
        print(f"Found: {title}")

print(f"\nTotal PDFs found: {len(pdf_links)}")

# Download each PDF
downloaded = 0
failed = 0

for i, pdf in enumerate(pdf_links, 1):
    try:
        # Create filename from URL
        filename = pdf['url'].split('/')[-1]
        
        # Remove query parameters
        if '?' in filename:
            filename = filename.split('?')[0]
        
        # Clean filename - remove any spaces or weird characters
        filename = filename.strip()
        
        # Ensure .pdf extension
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        
        filepath = output_dir / filename
        
        # Skip if exists
        if filepath.exists():
            print(f"[{i}/{len(pdf_links)}] ✓ Already exists: {filename}")
            downloaded += 1
            continue
        
        # Download
        print(f"[{i}/{len(pdf_links)}] Downloading: {filename}")
        
        # Make sure URL doesn't have trailing spaces
        clean_url = pdf['url'].strip()
        
        pdf_response = requests.get(clean_url, headers=headers, timeout=60, stream=True)
        pdf_response.raise_for_status()
        
        # Save
        with open(filepath, 'wb') as f:
            for chunk in pdf_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        size_kb = filepath.stat().st_size / 1024
        print(f"    ✓ Saved: {size_kb:.1f} KB")
        downloaded += 1
        
        time.sleep(0.5)  # Be nice to the server
        
    except requests.exceptions.HTTPError as e:
        print(f"    ✗ HTTP Error: {e}")
        failed += 1
    except Exception as e:
        print(f"    ✗ Error: {e}")
        failed += 1

print(f"\n{'='*60}")
print(f"✓ Successfully downloaded: {downloaded}/{len(pdf_links)}")
print(f"✗ Failed: {failed}")
print(f"{'='*60}")
print(f"\n✓ Done! Files saved to: {output_dir.absolute()}")