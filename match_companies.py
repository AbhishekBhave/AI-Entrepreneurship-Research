import pandas as pd
from name_matching.name_matcher import NameMatcher
import re
import os
from tqdm import tqdm
import sys

def preprocess_text(text):
    if pd.isna(text):
        return ""
    # Replace non-letters/numbers with space
    text = re.sub(r"[^A-Za-z0-9]", " ", str(text))
    # Collapse multiple spaces into one and strip
    text = re.sub(r"\s+", " ", text).strip()
    return text

def main():
    # Paths
    # Using absolute paths based on workspace structure
    cb_path = "/Users/abhishekbhave/Downloads/Name Matching/samplecrunchbase3.csv"
    pb_path = "/Users/abhishekbhave/Downloads/Name Matching/samplepitchbook3.csv"
    output_path = "matched_sample.csv"

    print("Loading data...", flush=True)
    # Load data
    if not os.path.exists(cb_path) or not os.path.exists(pb_path):
        print(f"Error: Input files not found in 'Name Matching' directory.", flush=True)
        # Fallback to local if running from root and paths are relative
        if os.path.exists(os.path.basename(cb_path)):
            cb_path = os.path.basename(cb_path)
        if os.path.exists(os.path.basename(pb_path)):
            pb_path = os.path.basename(pb_path)
            
    try:
        df_cb = pd.read_csv(cb_path)
        df_pb = pd.read_csv(pb_path)
    except Exception as e:
        print(f"Failed to read CSVs: {e}", flush=True)
        return

    # Sampling for initial run (as requested)
    SAMPLE_SIZE = 100
    print(f"Sampling first {SAMPLE_SIZE} rows from Pitchbook data for quick verification...", flush=True)
    df_pb_sample = df_pb.head(SAMPLE_SIZE).copy()
    # We keep full Crunchbase as master list to match against
    
    print("Preprocessing names...", flush=True)
    # Preprocessing
    df_cb["co1_clean"] = df_cb["co1"].apply(preprocess_text)
    df_pb_sample["co2_clean"] = df_pb_sample["co2"].apply(preprocess_text)

    print("Initializing NameMatcher...", flush=True)
    # Initialize NameMatcher
    try:
        matcher = NameMatcher(top_n=10,
            lowercase=True,
            punctuations=False,
            remove_ascii=True,
            legal_suffixes=True,
            common_words=True,
            verbose=True
        )
    except Exception as e:
        print(f"Failed to init matcher: {e}", flush=True)
        return

    # Set distance metrics
    matcher.set_distance_metrics(['discounted_levenshtein', 'SSK', 'fuzzy_wuzzy_token_sort'])

    print("Loading master data (Crunchbase)...", flush=True)
    # Load and process master data
    matcher.load_and_process_master_data(column='co1_clean', df_matching_data=df_cb, transform=True)

    print("Matching names (Pitchbook Sample)...", flush=True)
    # Perform matching
    matches = matcher.match_names(to_be_matched=df_pb_sample, column_matching='co2_clean')

    # Combine datasets
    print("Merging results...", flush=True)
    combined = pd.merge(df_pb_sample, matches, how='left', left_index=True, right_on='match_index')
    
    # Bring in the matched name from Crunchbase for comparison
    # The matches dataframe has 'match_name' which corresponds to the name in master data
    # But we might want other columns from Crunchbase
    
    # Add exact match check for verification
    combined['exact_match_check'] = combined['co2_clean'] == combined['match_name']

    # Select final columns
    # We want to KEEP idp, co2, match_name, score, exact_match_check, match_index
    # And DROP description2, state2, original_name
    # We can also keep co2_clean for reference if needed, or drop it. I'll keep it for debugging unless asked.
    
    final_cols = ['idp', 'co2', 'match_name', 'score', 'match_index', 'exact_match_check']
    # Ensure these columns exist
    final_cols = [c for c in final_cols if c in combined.columns]
    
    combined = combined[final_cols]
    
    print(f"Saving results to {output_path}...", flush=True)
    try:
        combined.to_csv(output_path, index=False)
        print("Successfully saved.", flush=True)
    except Exception as e:
        print(f"Failed to save CSV: {e}", flush=True)
    
    # Display sample results
    print("\nSample Matches:", flush=True)
    cols_to_show = ['co2', 'match_name', 'score', 'exact_match_check']
    print(combined[cols_to_show].head(10), flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"CRITICAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
