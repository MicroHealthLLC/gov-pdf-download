import pandas as pd
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import openpyxl
import numpy as np

def print_banner():
    """Display application banner"""
    print("=" * 70)
    print("  CSV/Excel to JSON Converter with LLM Context Preservation")
    print("  Multi-Tab Support | Configurable Segment Size | Interactive CLI")
    print("=" * 70)
    print()

def get_user_input(prompt: str, default: str = None) -> str:
    """Get user input with optional default value"""
    if default:
        user_input = input(f"{prompt} [default: {default}]: ").strip()
        return user_input if user_input else default
    return input(f"{prompt}: ").strip()

def get_numeric_input(prompt: str, default: float = None, min_val: float = None) -> float:
    """Get numeric input with validation"""
    while True:
        try:
            if default:
                value = input(f"{prompt} [default: {default}]: ").strip()
                value = float(value) if value else default
            else:
                value = float(input(f"{prompt}: ").strip())
            
            if min_val is not None and value < min_val:
                print(f"Error: Value must be at least {min_val}")
                continue
            return value
        except ValueError:
            print("Error: Please enter a valid number")

def get_files_from_directory(directory: str, extensions: List[str]) -> List[str]:
    """Get all files with specified extensions from directory"""
    files = []
    for ext in extensions:
        files.extend(list(Path(directory).glob(f"*{ext}")))
    return [str(f) for f in files]

def detect_excel_sheets(file_path: str) -> List[str]:
    """Detect all sheets in an Excel file"""
    try:
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            excel_file = pd.ExcelFile(file_path)
            return excel_file.sheet_names
        return []
    except Exception as e:
        print(f"Warning: Could not read sheets from {file_path}: {e}")
        return []

def clean_dataframe(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Clean dataframe by removing unnamed and empty columns
    
    Args:
        df: Input dataframe
        verbose: Print cleaning information
    
    Returns:
        Cleaned dataframe
    """
    original_cols = len(df.columns)
    
    # Step 1: Remove columns that are entirely unnamed and empty
    columns_to_keep = []
    columns_removed = []
    
    for col in df.columns:
        # Check if column name starts with "Unnamed:"
        is_unnamed = str(col).startswith('Unnamed:')
        
        # Check if column is entirely null/empty
        is_empty = df[col].isna().all() or (df[col].astype(str).str.strip() == '').all()
        
        # Calculate percentage of non-null values
        non_null_pct = (df[col].notna().sum() / len(df)) * 100
        
        # Keep column if:
        # 1. It's not unnamed, OR
        # 2. It's unnamed but has significant data (>5% non-null)
        if not is_unnamed or (is_unnamed and non_null_pct > 5):
            columns_to_keep.append(col)
        else:
            columns_removed.append({
                'name': col,
                'non_null_pct': non_null_pct,
                'reason': 'Unnamed and mostly empty'
            })
    
    # Create cleaned dataframe
    df_cleaned = df[columns_to_keep].copy()
    
    # Step 2: Remove rows that are entirely empty
    rows_before = len(df_cleaned)
    df_cleaned = df_cleaned.dropna(how='all')
    rows_removed = rows_before - len(df_cleaned)
    
    if verbose and (columns_removed or rows_removed > 0):
        print(f"\n{'='*70}")
        print("DATA CLEANING SUMMARY")
        print(f"{'='*70}")
        print(f"Original columns: {original_cols}")
        print(f"Columns removed: {len(columns_removed)}")
        print(f"Columns retained: {len(columns_to_keep)}")
        print(f"Empty rows removed: {rows_removed}")
        
        if columns_removed:
            print(f"\nRemoved columns:")
            for col_info in columns_removed[:10]:  # Limit to first 10
                print(f"  - {col_info['name']} ({col_info['non_null_pct']:.1f}% data)")
            if len(columns_removed) > 10:
                print(f"  ... and {len(columns_removed) - 10} more")
        
        print(f"\nRetained columns:")
        for col in df_cleaned.columns[:10]:  # Limit to first 10
            non_null_pct = (df_cleaned[col].notna().sum() / len(df_cleaned)) * 100
            print(f"  - {col} ({non_null_pct:.1f}% data)")
        if len(df_cleaned.columns) > 10:
            print(f"  ... and {len(df_cleaned.columns) - 10} more")
        print(f"{'='*70}\n")
    
    return df_cleaned

def convert_to_contextual_json(
    file_path: str,
    output_prefix: str,
    max_size_mb: float,
    sheet_name: str = None,
    include_metadata: bool = True,
    clean_data: bool = True,
    minify: bool = True
):
    """
    Convert CSV/Excel to JSON with preserved row and column context
    
    Args:
        file_path: Path to input file
        output_prefix: Prefix for output files
        max_size_mb: Maximum size per segment in MB
        sheet_name: Excel sheet name (None for CSV)
        include_metadata: Include file metadata in output
        clean_data: Remove unnamed/empty columns
        minify: Save JSON in minified format (no whitespace)
    """
    print(f"\n{'='*70}")
    print(f"Processing: {file_path}")
    if sheet_name:
        print(f"Sheet: {sheet_name}")
    print(f"{'='*70}\n")
    
    # Read the data
    try:
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            source_identifier = f"{Path(file_path).name}::{sheet_name}"
        else:
            df = pd.read_csv(file_path)
            source_identifier = Path(file_path).name
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    
    # Clean the dataframe if requested
    if clean_data:
        df = clean_dataframe(df, verbose=True)
        
        if len(df.columns) == 0:
            print("Warning: No valid columns remaining after cleaning. Skipping file.")
            return
        
        if len(df) == 0:
            print("Warning: No valid rows remaining after cleaning. Skipping file.")
            return
    
    # Calculate max size in bytes
    max_size_bytes = max_size_mb * 1024 * 1024
    
    segment_num = 1
    current_segment = []
    current_size = 0
    total_rows = len(df)
    
    # Prepare metadata
    metadata = {
        "source_file": str(file_path),
        "source_identifier": source_identifier,
        "total_rows_in_source": total_rows,
        "column_count": len(df.columns),
        "data_types": {col: str(df[col].dtype) for col in df.columns},
        "data_cleaned": clean_data,
        "minified": minify
    } if include_metadata else {}
    
    print(f"Total rows to process: {total_rows}")
    print(f"Columns: {len(df.columns)}")
    print(f"Target segment size: {max_size_mb} MB")
    print(f"Output format: {'Minified' if minify else 'Formatted'}\n")
    
    for index, row in df.iterrows():
        # Create a rich context object for each row
        row_object = {
            "row_index": int(index),
            "source_sheet": sheet_name if sheet_name else "N/A",
            "row_data": {}
        }
        
        # Embed column names with values for context preservation
        for column in df.columns:
            cell_value = row[column]
            
            # Handle NaN and None values
            if pd.isna(cell_value):
                cell_value = None
            elif isinstance(cell_value, (pd.Timestamp, pd.Timedelta)):
                cell_value = str(cell_value)
            elif isinstance(cell_value, (np.integer, np.floating)):
                cell_value = cell_value.item()  # Convert numpy types to Python types
            
            row_object["row_data"][column] = {
                "column_name": column,
                "value": cell_value,
                "data_type": str(type(cell_value).__name__),
                "original_dtype": str(df[column].dtype)
            }
        
        # Convert to JSON string to check size (using minified format for size calculation)
        row_json = json.dumps(row_object, ensure_ascii=False, separators=(',', ':'), default=str)
        row_size = len(row_json.encode('utf-8'))
        
        # Check if adding this row exceeds segment size
        if current_size + row_size > max_size_bytes and current_segment:
            # Write current segment
            write_segment(
                output_prefix,
                segment_num,
                current_segment,
                df.columns.tolist(),
                metadata,
                source_identifier,
                minify
            )
            
            # Reset for next segment
            segment_num += 1
            current_segment = []
            current_size = 0
        
        current_segment.append(row_object)
        current_size += row_size
        
        # Progress indicator
        if (index + 1) % 100 == 0:
            print(f"Processed {index + 1}/{total_rows} rows...", end='\r')
    
    # Write final segment
    if current_segment:
        write_segment(
            output_prefix,
            segment_num,
            current_segment,
            df.columns.tolist(),
            metadata,
            source_identifier,
            minify
        )
    
    print(f"\n✓ Completed processing {total_rows} rows into {segment_num} segment(s)\n")

def write_segment(
    output_prefix: str,
    segment_num: int,
    segment_data: List[Dict],
    column_schema: List[str],
    metadata: Dict[str, Any],
    source_identifier: str,
    minify: bool = True
):
    """
    Write a segment to JSON file
    
    Args:
        output_prefix: Prefix for output filename
        segment_num: Segment number
        segment_data: List of row objects
        column_schema: List of column names
        metadata: File metadata
        source_identifier: Source file identifier
        minify: If True, save in minified format; if False, use pretty-print
    """
    output_file = f"{output_prefix}_segment_{segment_num}.json"
    
    segment_content = {
        "segment_metadata": {
            "segment_number": segment_num,
            "total_rows_in_segment": len(segment_data),
            "source_identifier": source_identifier,
            "first_row_index": segment_data[0]["row_index"],
            "last_row_index": segment_data[-1]["row_index"]
        },
        "column_schema": column_schema,
        "file_metadata": metadata,
        "rows": segment_data
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        if minify:
            # Minified format: no whitespace, compact separators
            json.dump(segment_content, f, ensure_ascii=False, separators=(',', ':'), default=str)
        else:
            # Pretty-print format: indented, readable
            json.dump(segment_content, f, indent=2, ensure_ascii=False, default=str)
    
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"✓ Created: {output_file} ({file_size:.2f} MB)")

def process_file(file_path: str, output_dir: str, max_size_mb: float, clean_data: bool, minify: bool):
    """Process a single file (CSV or Excel with multiple tabs)"""
    file_name = Path(file_path).stem
    
    # Check if it's an Excel file with multiple sheets
    sheets = detect_excel_sheets(file_path)
    
    if sheets:
        print(f"\nDetected {len(sheets)} sheet(s) in Excel file:")
        for i, sheet in enumerate(sheets, 1):
            print(f"  {i}. {sheet}")
        
        choice = get_user_input(
            "\nProcess (A)ll sheets or (S)elect specific sheets?",
            "A"
        ).upper()
        
        sheets_to_process = sheets
        if choice == 'S':
            selected = get_user_input(
                "Enter sheet numbers separated by commas (e.g., 1,3,5)"
            )
            try:
                indices = [int(x.strip()) - 1 for x in selected.split(',')]
                sheets_to_process = [sheets[i] for i in indices if 0 <= i < len(sheets)]
            except:
                print("Invalid selection. Processing all sheets.")
        
        # Process each selected sheet
        for sheet in sheets_to_process:
            safe_sheet_name = sheet.replace(' ', '_').replace('/', '_')
            output_prefix = os.path.join(output_dir, f"{file_name}_{safe_sheet_name}")
            convert_to_contextual_json(file_path, output_prefix, max_size_mb, sheet, clean_data=clean_data, minify=minify)
    else:
        # Process as CSV
        output_prefix = os.path.join(output_dir, file_name)
        convert_to_contextual_json(file_path, output_prefix, max_size_mb, clean_data=clean_data, minify=minify)

def main():
    """Main interactive CLI function"""
    print_banner()
    
    # Step 1: Get input source (directory or file)
    print("STEP 1: Select Input Source")
    print("-" * 70)
    source_type = get_user_input(
        "Process (D)irectory or (F)ile?",
        "F"
    ).upper()
    
    files_to_process = []
    
    if source_type == 'D':
        directory = get_user_input("Enter directory path", ".")
        if not os.path.isdir(directory):
            print(f"Error: Directory '{directory}' not found")
            sys.exit(1)
        
        extensions = ['.csv', '.xlsx', '.xls']
        files_to_process = get_files_from_directory(directory, extensions)
        
        if not files_to_process:
            print(f"No CSV or Excel files found in '{directory}'")
            sys.exit(1)
        
        print(f"\nFound {len(files_to_process)} file(s):")
        for i, f in enumerate(files_to_process, 1):
            print(f"  {i}. {f}")
    else:
        file_path = get_user_input("Enter file path")
        if not os.path.isfile(file_path):
            print(f"Error: File '{file_path}' not found")
            sys.exit(1)
        files_to_process = [file_path]
    
    # Step 2: Get output directory
    print("\nSTEP 2: Output Configuration")
    print("-" * 70)
    output_dir = get_user_input("Enter output directory", "./json_output")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    print(f"✓ Output directory: {output_dir}")
    
    # Step 3: Get segment size
    print("\nSTEP 3: Segment Size Configuration")
    print("-" * 70)
    max_size_mb = get_numeric_input(
        "Enter maximum segment size in MB",
        default=4.5,
        min_val=0.1
    )
    
    # Step 4: Data cleaning option
    print("\nSTEP 4: Data Cleaning Configuration")
    print("-" * 70)
    clean_choice = get_user_input(
        "Automatically remove unnamed/empty columns? (Y/N)",
        "Y"
    ).upper()
    clean_data = clean_choice == 'Y'
    
    if clean_data:
        print("✓ Will automatically clean unnamed and empty columns")
    else:
        print("✓ Will preserve all columns as-is")
    
    # Step 5: JSON format option
    print("\nSTEP 5: JSON Format Configuration")
    print("-" * 70)
    print("Minified JSON removes all whitespace and indentation,")
    print("reducing file size by 30-50% but making it less human-readable.")
    minify_choice = get_user_input(
        "Save JSON in minified format? (Y/N)",
        "Y"
    ).upper()
    minify = minify_choice == 'Y'
    
    if minify:
        print("✓ Will save JSON in minified format (compact, smaller files)")
    else:
        print("✓ Will save JSON in formatted format (readable, larger files)")
    
    # Step 6: Process files
    print("\nSTEP 6: Processing Files")
    print("-" * 70)
    
    total_files = len(files_to_process)
    for idx, file_path in enumerate(files_to_process, 1):
        print(f"\n[File {idx}/{total_files}]")
        try:
            process_file(file_path, output_dir, max_size_mb, clean_data, minify)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            import traceback
            traceback.print_exc()
    
    # Calculate total output size
    total_size = 0
    output_files = list(Path(output_dir).glob("*.json"))
    for f in output_files:
        total_size += f.stat().st_size
    
    total_size_mb = total_size / (1024 * 1024)
    
    print("\n" + "=" * 70)
    print("  ✓ CONVERSION COMPLETE")
    print("=" * 70)
    print(f"\nOutput directory: {output_dir}")
    print(f"Total files created: {len(output_files)}")
    print(f"Total output size: {total_size_mb:.2f} MB")
    
    if minify:
        estimated_unminified = total_size_mb * 1.5  # Approximate 50% larger
        print(f"Estimated size if unminified: ~{estimated_unminified:.2f} MB")
        print(f"Space saved by minification: ~{estimated_unminified - total_size_mb:.2f} MB")
    
    print("\nThese JSON files are optimized for LLM vectorization with:")
    print("  • Preserved row and column context")
    print("  • Embedded column names with each value")
    print("  • Data type information for semantic understanding")
    print("  • Multi-tab support with sheet identification")
    print("  • Configurable segment sizes for optimal chunking")
    if clean_data:
        print("  • Automatic removal of unnamed/empty columns")
    if minify:
        print("  • Minified format for reduced file size")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
