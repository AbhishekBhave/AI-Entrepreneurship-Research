"""
Script to merge financial data with existing company data
"""
import pandas as pd
import sys

def merge_financial_data(
    companies_csv: str = "companies_2024.csv",
    financial_csv: str = "companies_financial_2024.csv",
    output_csv: str = "companies_with_financial_2024.csv"
):
    """Merge financial data with existing company data"""
    
    print(f"Loading companies from {companies_csv}...")
    try:
        companies_df = pd.read_csv(companies_csv)
        print(f"Loaded {len(companies_df)} companies")
    except FileNotFoundError:
        print(f"Error: {companies_csv} not found")
        sys.exit(1)
    
    print(f"Loading financial data from {financial_csv}...")
    try:
        financial_df = pd.read_csv(financial_csv)
        print(f"Loaded {len(financial_df)} companies with financial data")
    except FileNotFoundError:
        print(f"Error: {financial_csv} not found")
        print("Run crunchbase_financial_data.py first to fetch financial data")
        sys.exit(1)
    
    # Merge on UUID (preferred) or permalink
    merge_key = None
    if 'uuid' in companies_df.columns and 'uuid' in financial_df.columns:
        merge_key = 'uuid'
        print("Merging on UUID...")
    elif 'permalink' in companies_df.columns and 'permalink' in financial_df.columns:
        merge_key = 'permalink'
        print("Merging on permalink...")
    else:
        print("Error: No common key found for merging (need 'uuid' or 'permalink')")
        sys.exit(1)
    
    # Merge dataframes
    merged_df = companies_df.merge(
        financial_df,
        on=merge_key,
        how='left',
        suffixes=('', '_financial')
    )
    
    # Remove duplicate columns (keep original if both exist)
    for col in merged_df.columns:
        if col.endswith('_financial'):
            original_col = col.replace('_financial', '')
            if original_col in merged_df.columns:
                # Keep the financial version if it has more data
                if merged_df[col].notna().sum() > merged_df[original_col].notna().sum():
                    merged_df[original_col] = merged_df[col]
                merged_df = merged_df.drop(columns=[col])
    
    print(f"\nMerged dataset: {len(merged_df)} companies")
    print(f"Companies with financial data: {merged_df['funding_total_numeric'].notna().sum()}")
    
    # Save merged data
    merged_df.to_csv(output_csv, index=False)
    print(f"\nSaved merged data to {output_csv}")
    
    # Print summary
    print("\n=== Financial Data Coverage ===")
    financial_cols = [
        'funding_total_numeric', 'num_funding_rounds', 'last_funding_on',
        'revenue_range', 'valuation_numeric', 'num_investors'
    ]
    
    for col in financial_cols:
        if col in merged_df.columns:
            count = merged_df[col].notna().sum()
            pct = (count / len(merged_df)) * 100
            print(f"{col}: {count} companies ({pct:.1f}%)")
    
    return merged_df

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge financial data with company data")
    parser.add_argument(
        "--companies",
        default="companies_2024.csv",
        help="Path to companies CSV file"
    )
    parser.add_argument(
        "--financial",
        default="companies_financial_2024.csv",
        help="Path to financial data CSV file"
    )
    parser.add_argument(
        "--output",
        default="companies_with_financial_2024.csv",
        help="Output CSV file path"
    )
    
    args = parser.parse_args()
    
    merge_financial_data(args.companies, args.financial, args.output)

