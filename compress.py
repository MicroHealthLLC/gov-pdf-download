#!/usr/bin/env python3
"""
Minimal PDF Creator - Text Only Extraction
Creates minimal PDFs containing only text with '-minimal.pdf' suffix,
and deletes original files.
"""

import os
import sys
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("Required library not found. Install with:")
    print("pip install PyMuPDF")
    sys.exit(1)


def create_minimal_pdf(input_path, output_path):
    """
    Extract only text and create minimal PDF.
    
    Args:
        input_path: Path to input PDF
        output_path: Path to output minimal PDF
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Open the source PDF
        doc = fitz.open(input_path)
        
        # Create new PDF document
        writer = fitz.open()
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            
            # Create new page with same dimensions
            new_page = writer.new_page(
                width=page.rect.width,
                height=page.rect.height
            )
            
            # Extract text with position information
            text_dict = page.get_text("dict")
            
            # Re-insert text only (no images, no formatting)
            for block in text_dict["blocks"]:
                # Only process text blocks (type 0)
                if block.get("type") == 0:
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            # Insert text at original position
                            text = span.get("text", "")
                            bbox = span.get("bbox", [0, 0, 0, 0])
                            size = span.get("size", 11)
                            
                            # Use simple black text
                            new_page.insert_text(
                                point=(bbox[0], bbox[3]),
                                text=text,
                                fontsize=size,
                                color=(0, 0, 0),  # Black
                                fontname="helv"  # Helvetica (built-in)
                            )
        
        # Save with maximum compression
        writer.save(
            output_path,
            garbage=4,  # Maximum garbage collection
            deflate=True,  # Compress streams
            clean=True,  # Clean unused objects
            deflate_images=True,  # Compress any remaining images
            deflate_fonts=True  # Compress fonts
        )
        
        writer.close()
        doc.close()
        
        return True
        
    except Exception as e:
        print(f"Error creating minimal PDF from {input_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def process_directory(directory_path, delete_originals=True):
    """
    Process all PDFs in the specified directory.
    
    Args:
        directory_path: Path to directory containing PDFs
        delete_originals: Whether to delete original files after processing
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"Directory not found: {directory_path}")
        return
    
    # Find all PDF files
    pdf_files = list(directory.glob("*.pdf"))
    
    if not pdf_files:
        print("No PDF files found in directory.")
        return
    
    print(f"Found {len(pdf_files)} PDF file(s) to process.\n")
    
    successful = 0
    failed = 0
    
    for pdf_file in pdf_files:
        # Skip if already minimal (has -minimal in name)
        if "-minimal" in pdf_file.stem:
            print(f"Skipping (already minimal): {pdf_file.name}")
            continue
            
        print(f"Processing: {pdf_file.name}")
        
        # Create output filename with '-minimal' suffix (hyphen, not underscore)
        output_file = pdf_file.parent / f"{pdf_file.stem}-minimal.pdf"
        
        # Create minimal PDF
        if create_minimal_pdf(str(pdf_file), str(output_file)):
            original_size = pdf_file.stat().st_size
            minimal_size = output_file.stat().st_size
            reduction = ((original_size - minimal_size) / original_size) * 100
            
            print(f"  Original: {original_size / 1024:.2f} KB")
            print(f"  Minimal: {minimal_size / 1024:.2f} KB")
            print(f"  Reduction: {reduction:.1f}%")
            print(f"  Output: {output_file.name}")
            
            # Delete original if requested
            if delete_originals:
                try:
                    pdf_file.unlink()
                    print(f"  ✓ Original deleted\n")
                except Exception as e:
                    print(f"  ✗ Failed to delete original: {str(e)}\n")
            else:
                print(f"  ✓ Original kept\n")
            
            successful += 1
        else:
            failed += 1
            print(f"  ✗ Failed\n")
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"{'='*60}")


def main():
    """Main entry point for the script."""
    print("="*60)
    print("Minimal PDF Creator - Text Only Extraction")
    print("="*60)
    
    # Get directory from command line or use current directory
    if len(sys.argv) > 1:
        target_dir = sys.argv[1]
    else:
        target_dir = "."
    
    print(f"Target directory: {os.path.abspath(target_dir)}\n")
    
    # Confirm before proceeding
    print("WARNING: This will:")
    print("  1. Extract ONLY text from PDFs (all images will be removed)")
    print("  2. Create new files with '-minimal.pdf' suffix")
    print("     Example: document.pdf → document-minimal.pdf")
    print("  3. DELETE the original PDF files")
    print()
    
    response = input("Continue? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        process_directory(target_dir, delete_originals=True)
    else:
        print("\nOperation cancelled.")


if __name__ == "__main__":
    main()