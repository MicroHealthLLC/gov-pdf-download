import pandas as pd
import os
from pathlib import Path
import glob

def get_file_size_mb(filepath):
    return os.path.getsize(filepath) / (1024 * 1024)

def sanitize_name(name):
    invalid = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for char in invalid:
        name = name.replace(char, '_')
    return name.strip()

def split_sheet(df, sheet_name, base_name, output_dir, max_size_mb):
    if len(df) == 0:
        return 0
    
    safe_sheet = sanitize_name(sheet_name)
    safe_base = sanitize_name(base_name)
    
    # Estimate rows per segment
    sample = df.head(min(100, len(df)))
    temp_file = os.path.join(output_dir, "temp_test.xlsx")
    sample.to_excel(temp_file, index=False, engine='openpyxl')
    sample_size = get_file_size_mb(temp_file)
    os.remove(temp_file)
    
    rows_per_mb = len(sample) / sample_size if sample_size > 0 else 100
    chunk_size = max(10, int(rows_per_mb * max_size_mb * 0.85))
    
    segment = 1
    start = 0
    created = 0
    
    while start < len(df):
        end = min(start + chunk_size, len(df))
        chunk = df.iloc[start:end]
        
        filename = f"{safe_base}_{safe_sheet}_seg{segment}.xlsx"
        filepath = os.path.join(output_dir, filename)
        
        chunk.to_excel(filepath, index=False, engine='openpyxl')
        size = get_file_size_mb(filepath)
        
        print(f"  ✓ {filename} ({size:.2f} MB, {len(chunk)} rows)")
        
        start = end
        segment += 1
        created += 1
    
    return created

def process_all_excel_files(max_size_mb=4):
    current_dir = os.getcwd()
    excel_files = glob.glob("*.xlsx") + glob.glob("*.xls")
    
    if not excel_files:
        print("❌ No Excel files found in current directory!")
        return
    
    print(f"\n{'='*60}")
    print(f"Found {len(excel_files)} Excel file(s) in: {current_dir}")
    print(f"Target segment size: {max_size_mb} MB")
    print(f"{'='*60}\n")
    
    for excel_file in excel_files:
        print(f"\n📁 Processing: {excel_file}")
        print(f"   Original size: {get_file_size_mb(excel_file):.2f} MB")
        
        try:
            xls = pd.ExcelFile(excel_file, engine='openpyxl')
            base_name = Path(excel_file).stem
            
            total_segments = 0
            
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')
                
                if len(df) > 0:
                    print(f"\n  📋 Sheet: '{sheet_name}' ({len(df)} rows)")
                    segs = split_sheet(df, sheet_name, base_name, current_dir, max_size_mb)
                    total_segments += segs
            
            print(f"\n  ✅ Done! Created {total_segments} segments from {excel_file}\n")
            print("-" * 60)
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}\n")
            continue
    
    print(f"\n{'='*60}")
    print("🎉 ALL FILES PROCESSED!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    # Just run it - processes all Excel files in current directory
    process_all_excel_files(max_size_mb=4)
