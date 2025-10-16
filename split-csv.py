import pandas as pd
import os
import sys
from pathlib import Path

def get_file_size_mb(filepath):
    """Get file size in MB"""
    return os.path.getsize(filepath) / (1024 * 1024)

def estimate_row_size(df, file_extension, sample_size=100):
    """
    Estimate the average size per row by writing a sample
    Handles empty DataFrames
    """
    import tempfile
    
    if len(df) == 0:
        return 0.001  # 1KB per row as fallback
    
    sample_size = min(sample_size, len(df))
    sample_df = df.head(sample_size)
    
    with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp:
        temp_path = tmp.name
    
    try:
        if file_extension == '.csv':
            sample_df.to_csv(temp_path, index=False)
        else:
            sample_df.to_excel(temp_path, index=False, engine='openpyxl')
        
        sample_size_mb = get_file_size_mb(temp_path)
        avg_row_size_mb = sample_size_mb / sample_size
        
        return avg_row_size_mb
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

def analyze_excel_file(input_file):
    """
    Analyze Excel file for sheets, data, and metadata
    Returns dict with sheet info
    """
    print("\n" + "="*60)
    print("EXCEL FILE ANALYSIS")
    print("="*60)
    
    try:
        excel_file = pd.ExcelFile(input_file, engine='openpyxl')
        sheet_names = excel_file.sheet_names
        
        print(f"Number of sheets: {len(sheet_names)}")
        print(f"Sheet names: {', '.join(sheet_names)}")
        print()
        
        sheet_info = {}
        total_rows = 0
        
        for sheet_name in sheet_names:
            df = pd.read_excel(input_file, sheet_name=sheet_name, engine='openpyxl')
            rows = len(df)
            cols = len(df.columns)
            total_rows += rows
            
            sheet_info[sheet_name] = {'rows': rows, 'cols': cols, 'df': df}
            
            print(f"Sheet '{sheet_name}':")
            print(f"  - Rows: {rows}")
            print(f"  - Columns: {cols}")
            if rows > 0:
                print(f"  - Column names: {', '.join(df.columns.astype(str).tolist()[:5])}")
                if len(df.columns) > 5:
                    print(f"    ... and {len(df.columns) - 5} more columns")
            print()
        
        print(f"Total data rows across all sheets: {total_rows}")
        print("="*60 + "\n")
        
        return sheet_info
        
    except Exception as e:
        print(f"Error analyzing Excel file: {str(e)}")
        return None

def split_sheet(df, sheet_name, base_name, output_dir, file_extension, max_size_mb):
    """
    Split a single sheet/dataframe into segments
    """
    if len(df) == 0:
        print(f"⚠️  Skipping sheet '{sheet_name}' - no data rows")
        return 0
    
    total_rows = len(df)
    print(f"\n{'='*60}")
    print(f"Processing sheet: '{sheet_name}'")
    print(f"Total rows: {total_rows}")
    print(f"{'='*60}")
    
    # Estimate row size
    print("Estimating row size...")
    avg_row_size_mb = estimate_row_size(df, file_extension)
    print(f"Estimated average row size: {avg_row_size_mb * 1024:.2f} KB")
    
    # Calculate initial chunk size with safety margin
    safety_margin = 0.85
    estimated_chunk_size = int((max_size_mb * safety_margin) / avg_row_size_mb)
    estimated_chunk_size = max(10, estimated_chunk_size)
    
    print(f"Estimated chunk size: {estimated_chunk_size} rows")
    print(f"Maximum segment size: {max_size_mb} MB")
    print("-" * 50)
    
    segment_num = 1
    start_row = 0
    segments_created = 0
    
    while start_row < total_rows:
        chunk_size = estimated_chunk_size
        final_output = None
        
        # Binary search for optimal chunk size
        min_chunk = 10
        max_chunk = min(estimated_chunk_size * 2, total_rows - start_row)
        
        while min_chunk <= max_chunk:
            chunk_size = (min_chunk + max_chunk) // 2
            end_row = min(start_row + chunk_size, total_rows)
            chunk_df = df.iloc[start_row:end_row]
            
            # Create temporary output file
            if file_extension == '.csv':
                temp_output = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}_temp.csv"
                chunk_df.to_csv(temp_output, index=False)
            else:
                temp_output = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}_temp.xlsx"
                chunk_df.to_excel(temp_output, index=False, engine='openpyxl')
            
            file_size = get_file_size_mb(temp_output)
            
            if file_size <= max_size_mb:
                if final_output and os.path.exists(final_output):
                    os.remove(final_output)
                final_output = temp_output
                min_chunk = chunk_size + 1
            else:
                os.remove(temp_output)
                max_chunk = chunk_size - 1
        
        if final_output is None:
            print(f"Warning: Even minimum rows exceed {max_size_mb}MB. Using 10 rows.")
            end_row = min(start_row + 10, total_rows)
            chunk_df = df.iloc[start_row:end_row]
            
            if file_extension == '.csv':
                final_output = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}.csv"
                chunk_df.to_csv(final_output, index=False)
            else:
                final_output = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}.xlsx"
                chunk_df.to_excel(final_output, index=False, engine='openpyxl')
        else:
            if file_extension == '.csv':
                final_name = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}.csv"
            else:
                final_name = output_dir / f"{base_name}_{sheet_name}_segment_{segment_num}.xlsx"
            
            os.rename(final_output, final_name)
            final_output = final_name
            end_row = start_row + chunk_size
        
        final_size = get_file_size_mb(final_output)
        print(f"Created: {final_output.name}")
        print(f"  Rows: {start_row + 1} to {end_row} ({end_row - start_row} rows)")
        print(f"  Size: {final_size:.2f} MB")
        print()
        
        start_row = end_row
        segment_num += 1
        segments_created += 1
    
    return segments_created

def split_file(input_file, max_size_mb=4, sheet_name=None):
    """
    Split CSV or Excel file into segments not exceeding max_size_mb
    
    Args:
        input_file: Path to input CSV or Excel file
        max_size_mb: Maximum size per segment in MB (default: 4)
        sheet_name: Specific sheet name to process, or "ALL" for all sheets
    """
    file_extension = Path(input_file).suffix.lower()
    
    print(f"Reading {input_file}...")
    print(f"Original file size: {get_file_size_mb(input_file):.2f} MB")
    
    base_name = Path(input_file).stem
    output_dir = Path(input_file).parent
    
    # Handle Excel files
    if file_extension in ['.xlsx', '.xls']:
        sheet_info = analyze_excel_file(input_file)
        
        if sheet_info is None or len(sheet_info) == 0:
            print("⚠️  No sheets found or error reading file")
            return
        
        # Check if any sheet has data
        has_data = any(info['rows'] > 0 for info in sheet_info.values())
        if not has_data:
            print("⚠️  WARNING: No data rows found in any sheet!")
            return
        
        # Determine which sheets to process
        if sheet_name and sheet_name.upper() == "ALL":
            sheets_to_process = list(sheet_info.keys())
            print(f"\n📋 Processing ALL {len(sheets_to_process)} sheet(s)")
        elif sheet_name and sheet_name in sheet_info:
            sheets_to_process = [sheet_name]
            print(f"\n📋 Processing sheet: '{sheet_name}'")
        elif sheet_name:
            print(f"⚠️  ERROR: Sheet '{sheet_name}' not found!")
            print(f"Available sheets: {', '.join(sheet_info.keys())}")
            return
        elif len(sheet_info) == 1:
            sheets_to_process = list(sheet_info.keys())
            print(f"\n📋 Processing single sheet: '{sheets_to_process[0]}'")
        else:
            print("\n⚠️  Multiple sheets detected. Please specify:")
            for i, name in enumerate(sheet_info.keys(), 1):
                print(f"  {i}. {name}")
            print("\nOptions:")
            print("  - Process specific sheet: python split_file.py <file> <max_size_mb> '<sheet_name>'")
            print("  - Process ALL sheets: python split_file.py <file> <max_size_mb> ALL")
            return
        
        # Process each selected sheet
        total_segments = 0
        for sheet in sheets_to_process:
            if sheet_info[sheet]['rows'] > 0:
                segments = split_sheet(
                    sheet_info[sheet]['df'],
                    sheet,
                    base_name,
                    output_dir,
                    file_extension,
                    max_size_mb
                )
                total_segments += segments
        
        print("\n" + "="*60)
        print(f"✓ Split complete!")
        print(f"✓ Processed {len(sheets_to_process)} sheet(s)")
        print(f"✓ Created {total_segments} total segment(s)")
        print(f"✓ All segments are ≤ {max_size_mb} MB")
        print("="*60)
    
    # Handle CSV files
    elif file_extension == '.csv':
        df = pd.read_csv(input_file)
        
        if len(df) == 0:
            print("\n⚠️  ERROR: CSV file has 0 data rows!")
            return
        
        segments = split_sheet(df, "csv", base_name, output_dir, file_extension, max_size_mb)
        
        print("-" * 50)
        print(f"✓ Split complete! Created {segments} segment(s)")
        print(f"✓ All segments are ≤ {max_size_mb} MB")
    
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python split_file.py <input_file> [max_size_mb] [sheet_name|ALL]")
        print("\nExamples:")
        print("  python split_file.py data.xlsx 4")
        print("  python split_file.py data.xlsx 4 'Sheet1'")
        print("  python split_file.py data.xlsx 4 ALL")
        sys.exit(1)
    
    input_file = sys.argv[1]
    max_size = float(sys.argv[2]) if len(sys.argv) > 2 else 4.0
    sheet_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        split_file(input_file, max_size, sheet_name)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)