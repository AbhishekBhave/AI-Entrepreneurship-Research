import csv, time, requests, sys, json

API_KEY = "4096bc83cf155a0e85a37a4b350a16c1"
URL = f"https://api.crunchbase.com/v4/data/searches/organizations?user_key={API_KEY}"

FIELD_IDS = [
    "identifier",
    "founded_on",
    "short_description",
    "website",
    "num_employees_enum",
    "categories"
]

BODY_BASE = {
    "field_ids": FIELD_IDS,
    "order": [{"field_id": "founded_on", "sort": "asc"}],
    "query": [
        # Use two date predicates instead of a "between" for dates
        {"type": "predicate", "field_id": "founded_on", "operator_id": "gte", "values": ["2024-01-01"]},
        {"type": "predicate", "field_id": "founded_on", "operator_id": "lte", "values": ["2024-12-31"]},
        {"type": "predicate", "field_id": "facet_ids", "operator_id": "includes", "values": ["company"]}
    ],
    "limit": 1000
}

def flatten(e):
    p = e.get("properties", {}) or {}
    ident = p.get("identifier") or {}
    # Extract category names - handle both dict and string formats
    categories = p.get("categories") or []
    category_names = []
    for cat in categories:
        if isinstance(cat, dict):
            # Try common field names for category value
            category_names.append(cat.get("value") or cat.get("name") or str(cat))
        else:
            category_names.append(str(cat))
    return {
        "uuid": ident.get("uuid"),
        "name": ident.get("value"),
        "permalink": ident.get("permalink"),
        "founded_on": p.get("founded_on"),
        "short_description": p.get("short_description"),
        "website": p.get("website"),
        "num_employees_enum": p.get("num_employees_enum"),
        "categories": ";".join(category_names),
    }

def fetch_all():
    rows = []
    after_id = None
    s = requests.Session()
    headers = {"Content-Type": "application/json"}

    while True:
        body = dict(BODY_BASE)
        if after_id:
            body["after_id"] = after_id

        r = s.post(URL, json=body, headers=headers)

        # Helpful diagnostics if the API returns a 4xx/5xx
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

        rows.extend(flatten(e) for e in entities)

        # done if fewer than limit
        if len(entities) < BODY_BASE["limit"]:
            break

        after_id = entities[-1]["properties"]["identifier"]["uuid"]
        time.sleep(0.2)

    return rows

if __name__ == "__main__":
    all_rows = fetch_all()
    out_path = "companies_2024.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        headers = ["uuid","name","permalink","founded_on","short_description",
                   "website","num_employees_enum","categories"]
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)
    print(f"Saved {len(all_rows)} rows to {out_path}")