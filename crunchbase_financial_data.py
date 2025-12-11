import csv, time, requests, sys, json
from typing import List, Dict, Optional
import os

# API key should be set as environment variable: CRUNCHBASE_API_KEY
API_KEY = os.getenv("CRUNCHBASE_API_KEY", "YOUR_API_KEY_HERE")
URL = f"https://api.crunchbase.com/v4/data/searches/organizations?user_key={API_KEY}"

# Financial field IDs - using only verified available fields
FINANCIAL_FIELD_IDS = [
    "identifier",
    "founded_on",
    "short_description",
    "website",
    "num_employees_enum",
    "categories",
    # Financial fields - using basic fields that are more likely to be available
    "funding_total",
    "num_funding_rounds",
]

BODY_BASE = {
    "field_ids": FINANCIAL_FIELD_IDS,
    "order": [{"field_id": "founded_on", "sort": "asc"}],
    "query": [
        {"type": "predicate", "field_id": "founded_on", "operator_id": "gte", "values": ["2024-01-01"]},
        {"type": "predicate", "field_id": "founded_on", "operator_id": "lte", "values": ["2024-12-31"]},
        {"type": "predicate", "field_id": "facet_ids", "operator_id": "includes", "values": ["company"]}
    ],
    "limit": 1000
}

def safe_get(data: dict, *keys, default=None):
    """Safely get nested dictionary values"""
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key)
        else:
            return default
        if result is None:
            return default
    return result

def parse_funding_amount(amount_str: Optional[str]) -> Optional[float]:
    """Parse funding amount string to float (handles currency symbols, commas, etc.)"""
    if not amount_str:
        return None
    try:
        # Remove currency symbols, commas, and whitespace
        cleaned = str(amount_str).replace('$', '').replace(',', '').replace(' ', '').strip()
        # Handle ranges like "1000000-5000000" by taking the midpoint
        if '-' in cleaned:
            parts = cleaned.split('-')
            if len(parts) == 2:
                try:
                    low = float(parts[0])
                    high = float(parts[1])
                    return (low + high) / 2
                except:
                    return None
        return float(cleaned)
    except:
        return None

def flatten_financial(e: Dict) -> Dict:
    """Extract financial data from API response entity"""
    p = e.get("properties", {}) or {}
    ident = p.get("identifier") or {}
    
    # Extract category names
    categories = p.get("categories") or []
    category_names = []
    for cat in categories:
        if isinstance(cat, dict):
            category_names.append(cat.get("value") or cat.get("name") or str(cat))
        else:
            category_names.append(str(cat))
    
    # Extract financial data - simplified to only available fields
    funding_total = safe_get(p, "funding_total", "value")
    num_funding_rounds = p.get("num_funding_rounds")
    
    # Parse numeric values
    funding_total_numeric = parse_funding_amount(funding_total)
    
    return {
        # Basic info
        "uuid": ident.get("uuid"),
        "name": ident.get("value"),
        "permalink": ident.get("permalink"),
        "founded_on": p.get("founded_on"),
        "short_description": p.get("short_description"),
        "website": p.get("website"),
        "num_employees_enum": p.get("num_employees_enum"),
        "categories": ";".join(category_names),
        
        # Financial metrics (simplified)
        "funding_total": funding_total,
        "funding_total_numeric": funding_total_numeric,
        "num_funding_rounds": num_funding_rounds,
    }

def fetch_all_financial():
    """Fetch all companies with financial data"""
    rows = []
    after_id = None
    s = requests.Session()
    headers = {"Content-Type": "application/json"}

    while True:
        body = dict(BODY_BASE)
        if after_id:
            body["after_id"] = after_id

        r = s.post(URL, json=body, headers=headers)

        if not r.ok:
            sys.stderr.write(
                f"\n--- HTTP {r.status_code} ---\n"
                f"Request body:\n{json.dumps(body, indent=2)}\n"
                f"Response text:\n{r.text}\n"
            )
            r.raise_for_status()

        data = r.json()
        entities = data.get("entities") or []
        if not entities:
            break

        rows.extend(flatten_financial(e) for e in entities)

        # done if fewer than limit
        if len(entities) < BODY_BASE["limit"]:
            break

        after_id = entities[-1]["properties"]["identifier"]["uuid"]
        time.sleep(0.2)  # Rate limiting

    return rows

def fetch_financial_for_existing_companies(companies_csv_path: str = "companies_2024.csv"):
    """Fetch financial data by re-querying the search API with financial fields"""
    import pandas as pd
    
    # Read existing companies
    try:
        df = pd.read_csv(companies_csv_path)
        print(f"Loaded {len(df)} companies from {companies_csv_path}")
    except FileNotFoundError:
        print(f"File {companies_csv_path} not found. Fetching all companies with financial data...")
        return fetch_all_financial()
    
    print("Re-querying API with financial fields included...")
    print("(This will fetch all 2024 companies again, but with financial data)")
    
    # Use the search endpoint which works reliably
    return fetch_all_financial()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch financial data from Crunchbase API")
    parser.add_argument(
        "--mode",
        choices=["all", "existing"],
        default="all",
        help="Fetch all companies (all) or only existing companies from CSV (existing)"
    )
    parser.add_argument(
        "--input-csv",
        default="companies_2024.csv",
        help="Path to existing companies CSV file (for 'existing' mode)"
    )
    parser.add_argument(
        "--output-csv",
        default="companies_financial_2024.csv",
        help="Output CSV file path"
    )
    
    args = parser.parse_args()
    
    if args.mode == "existing":
        print("Fetching financial data for existing companies...")
        all_rows = fetch_financial_for_existing_companies(args.input_csv)
    else:
        print("Fetching all companies with financial data...")
        all_rows = fetch_all_financial()
    
    out_path = args.output_csv
    if all_rows:
        headers = list(all_rows[0].keys())
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in all_rows:
                w.writerow(r)
        print(f"Saved {len(all_rows)} rows with financial data to {out_path}")
        
        # Print summary statistics
        print("\n=== Financial Data Summary ===")
        funding_companies = sum(1 for r in all_rows if r.get("funding_total_numeric"))
        print(f"Companies with funding data: {funding_companies} ({funding_companies/len(all_rows)*100:.1f}%)")
        
        rounds_companies = sum(1 for r in all_rows if r.get("num_funding_rounds"))
        print(f"Companies with funding rounds data: {rounds_companies} ({rounds_companies/len(all_rows)*100:.1f}%)")
        
        # Calculate total funding
        total_funding = sum(r.get("funding_total_numeric") or 0 for r in all_rows)
        if total_funding > 0:
            print(f"Total funding across all companies: ${total_funding:,.0f}")
        
        # Average funding
        funded_companies = [r.get("funding_total_numeric") for r in all_rows if r.get("funding_total_numeric")]
        if funded_companies:
            avg_funding = sum(funded_companies) / len(funded_companies)
            print(f"Average funding (for companies with funding): ${avg_funding:,.0f}")
    else:
        print("No data fetched. Check API key and query parameters.")

