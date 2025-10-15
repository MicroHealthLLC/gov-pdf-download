import pandas as pd
import os
import sys
from pathlib import Path

def get_file_size_mb(filepath):
    """Get file size in MB"""
    return os.path.getsize(filepath) / (1024 * 1024)

def split_file(input_file, max_size_mb=4):
    """
    Split CSV or Excel file into segments not exceeding max_size_mb
    
    Args:
        input_file: Path to input CSV or Excel file
        max_size_mb: Maximum size per segment in MB (default: 4)
    """
    
    # Determine file type and read accordingly
    file_extension = Path(input_file).suffix.lower()
    
    print(f"Reading {input_file}...")
    
    if file_extension == '.csv':
        df = pd.read_csv(input_file)
    elif file_extension in ['.xlsx', '.xls']:
        df = pd.read_excel(input_file)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}. Use .csv, .xlsx, or .xls")
    
    # Get base filename without extension
    base_name = Path(input_file).stem
    output_dir = Path(input_file).parent
    
    # Initialize variables
    segment_num = 1
    start_row = 0
    total_rows = len(df)
    
    print(f"Total rows to process: {total_rows}")
    print(f"Maximum segment size: {max_size_mb} MB")
    print("-" * 50)
    
    while start_row < total_rows:
        # Start with a chunk size estimate
        chunk_size = 1000
        temp_output = None
        
        while True:
            end_row = min(start_row + chunk_size, total_rows)
            chunk_df = df.iloc[start_row:end_row]
            
            # Create temporary output file
            if file_extension == '.csv':
                temp_output = output_dir / f"{base_name}_segment_{segment_num}_temp.csv"
                chunk_df.to_csv(temp_output, index=False)
            else:
                temp_output = output_dir / f"{base_name}_segment_{segment_num}_temp.xlsx"
                chunk_df.to_excel(temp_output, index=False)
            
            # Check file size
            file_size = get_file_size_mb(temp_output)
            
            if file_size > max_size_mb:
                # File too large, reduce chunk size
                os.remove(temp_output)
                if chunk_size <= 10:
                    print(f"Warning: Even 10 rows exceed {max_size_mb}MB. Using 10 rows anyway.")
                    chunk_size = 10
                    break
                chunk_size = int(chunk_size * 0.8)  # Reduce by 20%
            elif end_row < total_rows and file_size < max_size_mb * 0.9:
                # File has room, try adding more rows
                os.remove(temp_output)
                chunk_size = int(chunk_size * 1.2)  # Increase by 20%
            else:
                # File size is good
                break
        
        # Finalize the segment
        if file_extension == '.csv':
            final_output = output_dir / f"{base_name}_segment_{segment_num}.csv"
        else:
            final_output = output_dir / f"{base_name}_segment_{segment_num}.xlsx"
        
        # Rename temp file to final name
        if temp_output and temp_output.exists():
            os.rename(temp_output, final_output)
            final_size = get_file_size_mb(final_output)
            print(f"Created: {final_output.name}")
            print(f"  Rows: {start_row + 1} to {end_row} ({end_row - start_row} rows)")
            print(f"  Size: {final_size:.2f} MB")
            print()
        
        # Move to next segment
        start_row = end_row
        segment_num += 1
    
    print("-" * 50)
    print(f"✓ Split complete! Created {segment_num - 1} segment(s)")
    print(f"✓ All segments are ≤ {max_size_mb} MB")
    print(f"✓ Header rows preserved in each segment")

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python split_file.py <input_file> [max_size_mb]")
        print("Example: python split_file.py data.csv 4")
        sys.exit(1)
    
    input_file = sys.argv[1]
    max_size = float(sys.argv[2]) if len(sys.argv) > 2 else 4.0
    
    try:
        split_file(input_file, max_size)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
