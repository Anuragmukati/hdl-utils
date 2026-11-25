"""
Run a JQL query against Jira and export results to a CSV file.

Usage:
    pip install requests
    python jira_jql_to_csv.py
"""

import csv
import sys
import requests

# ─────────────────────────── CONFIGURATION ───────────────────────────
JIRA_URL   = ""   # your Jira base URL
JIRA_TOKEN = ""    # Jira PAT  (or use email+api_token for cloud)
JQL        = ''
OUTPUT_CSV = "jira_results.csv"

# Fields to pull — add/remove Jira field names as needed
FIELDS = [
    "summary",
    "status",
    "assignee",
    "reporter",
    "priority",
    "created",
    "updated",
    "issuetype",
    "labels",
    "description",
]

MAX_RESULTS = 1000   # total cap; script auto-paginates in batches of 100
# ──────────────────────────────────────────────────────────────────────


def build_headers() -> dict:
    return {
        "Authorization": f"Bearer {JIRA_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def fetch_issues(jql: str, fields: list[str], max_results: int) -> list[dict]:
    """Page through Jira search API and return all matching issues."""
    url = f"{JIRA_URL}/rest/api/2/search"
    headers = build_headers()
    batch_size = 100
    start_at = 0
    all_issues: list[dict] = []

    while len(all_issues) < max_results:
        payload = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": min(batch_size, max_results - len(all_issues)),
            "fields": fields,
        }

        resp = requests.post(url, json=payload, headers=headers, timeout=60)

        if resp.status_code == 401:
            sys.exit("ERROR: Unauthorized — check your JIRA_TOKEN.")
        if resp.status_code != 200:
            sys.exit(f"ERROR {resp.status_code}: {resp.text[:400]}")

        data = resp.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)

        total = data.get("total", 0)
        start_at += len(issues)

        print(f"  Fetched {len(all_issues)} / {min(total, max_results)} issues...", end="\r")

        if start_at >= total or not issues:
            break

    print()   # newline after progress line
    return all_issues


def extract_value(field_name: str, field_val) -> str:
    """Flatten a Jira field value to a plain string."""
    if field_val is None:
        return ""
    if isinstance(field_val, str):
        return field_val
    if isinstance(field_val, list):
        return "; ".join(str(v.get("name", v) if isinstance(v, dict) else v) for v in field_val)
    if isinstance(field_val, dict):
        # common nested objects: status, assignee, reporter, priority, issuetype
        for key in ("displayName", "name", "value"):
            if key in field_val:
                return str(field_val[key])
    return str(field_val)


def issues_to_csv(issues: list[dict], fields: list[str], output_path: str) -> None:
    headers = ["key"] + fields

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for issue in issues:
            row = {"key": issue["key"]}
            for field in fields:
                raw = issue.get("fields", {}).get(field)
                row[field] = extract_value(field, raw)
            writer.writerow(row)

    print(f"Saved {len(issues)} rows → {output_path}")


def main():
    print(f"Running JQL: {JQL}")
    issues = fetch_issues(JQL, FIELDS, MAX_RESULTS)
    print(f"Total fetched: {len(issues)}")
    issues_to_csv(issues, FIELDS, OUTPUT_CSV)


if __name__ == "__main__":
    main()
