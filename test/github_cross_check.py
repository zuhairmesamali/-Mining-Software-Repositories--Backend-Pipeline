import requests
import datetime
import re
from urllib.parse import urlparse
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def get_repo_metrics_from_links(repo_links, token=None):
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    results = {}
    for url in repo_links:
        print(f"Processing repository URL: {url}")
        # Parse the GitHub repository URL to extract owner and repository name.
        try:
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip("/").split("/")
            if len(path_parts) < 2:
                raise ValueError
            owner, repo_name = path_parts[0], path_parts[1]
            print(f"Extracted owner: {owner}, repo_name: {repo_name}")
        except Exception:
            print(f"Invalid GitHub URL format: {url}")
            results[url] = {"error": "Invalid GitHub URL format. Expected format: https://github.com/owner/repo"}
            continue
        
        # Retrieve repository details.
        repo_api_url = f"https://api.github.com/repos/{owner}/{repo_name}"
        print(f"Fetching repository details from: {repo_api_url}")
        repo_response = requests.get(repo_api_url, headers=headers)
        if repo_response.status_code != 200:
            print(f"Failed to fetch repository details for {url}: {repo_response.text}")
            results[url] = {"error": f"Failed to fetch repository details: {repo_response.text}"}
            continue
        
        repo_data = repo_response.json()
        
        # Calculate project age (in days).
        try:
            created_at = datetime.datetime.strptime(repo_data.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ")
            now = datetime.datetime.utcnow()
            age_days = (now - created_at).days
            print(f"Project age (days): {age_days}")
        except Exception:
            print(f"Failed to calculate project age for {url}")
            age_days = None
        
        # Determine number of commits using the commits API.
        commits_api_url = f"https://api.github.com/repos/{owner}/{repo_name}/commits"
        print(f"Fetching commits from: {commits_api_url}")
        params = {
            "per_page": 1,
            "until": "2021-04-01T00:00:00Z"
        }
        commits_response = requests.get(commits_api_url, headers=headers, params=params)
        if commits_response.status_code != 200:
            print(f"Failed to fetch commits for {url}")
            commit_count = None
        else:
            if "Link" in commits_response.headers:
                link_header = commits_response.headers["Link"]
                match = re.search(r'page=(\d+)>; rel="last"', link_header)
                commit_count = int(match.group(1)) if match else 1
            else:
                commit_count = len(commits_response.json())
            print(f"Number of commits: {commit_count}")
        
        # Approximate total lines of code using the Languages API.
        languages_api_url = f"https://api.github.com/repos/{owner}/{repo_name}/languages"
        print(f"Fetching languages from: {languages_api_url}")
        languages_response = requests.get(languages_api_url, headers=headers)
        if languages_response.status_code == 200:
            language_data = languages_response.json()
            total_bytes = sum(language_data.values())
            total_lines = total_bytes // 50  # Approximation: 50 bytes per line.
            print(f"Total lines of code (approx): {total_lines}")
        else:
            print(f"Failed to fetch languages for {url}")
            total_lines = None
        
        # Retrieve number of contributing developers.
        contributors_api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contributors"
        print(f"Fetching contributors from: {contributors_api_url}")
        contributors_response = requests.get(contributors_api_url, headers=headers, params={"per_page": 100})
        if contributors_response.status_code == 200:
            contributors = contributors_response.json()
            contributor_count = len(contributors)
            print(f"Number of contributing developers: {contributor_count}")
        else:
            print(f"Failed to fetch contributors for {url}")
            contributor_count = None
        
        results[url] = {
            "project_age_days": age_days,
            "number_of_commits": commit_count,
            "total_lines_of_code": total_lines,
            "number_of_contributing_developers": contributor_count
        }
    
    return results

def get_github_repo_links_from_csv(csv_file_path, origin_column='origin'):
    print(f"Reading CSV file: {csv_file_path}")
    df = pd.read_csv(csv_file_path)
    # Filter rows where the origin column contains 'github.com'
    df_github = df[df[origin_column].astype(str).str.contains("github.com", na=False)]
    print(f"Found {len(df_github)} GitHub links in the CSV file.")
    # Clean whitespace in the URLs.
    df_github[origin_column] = df_github[origin_column].astype(str).str.strip()
    return df_github

def update_csv_with_metrics(csv_file_path, output_file_path, origin_column='origin', token=None):
    print(f"Starting to update CSV file: {csv_file_path}")
    # Extract valid GitHub repository links from the CSV file.
    df = get_github_repo_links_from_csv(csv_file_path, origin_column=origin_column)
    repo_links = df[origin_column].tolist()
    
    # Get metrics for each repository.
    print("Fetching metrics for repositories...")
    metrics_dict = get_repo_metrics_from_links(repo_links, token=token)
    
    # Initialize new columns with default values (None).
    df["project_age_days"] = None
    df["number_of_commits"] = None
    df["total_lines_of_code"] = None
    df["number_of_contributing_developers"] = None
    
    # Update the DataFrame with the metrics for each repository.
    for idx, row in df.iterrows():
        repo_url = row[origin_column]
        if repo_url in metrics_dict:
            repo_metrics = metrics_dict[repo_url]
            # Only add metrics if retrieval was successful (i.e., no error key).
            if "error" not in repo_metrics:
                df.at[idx, "project_age_days"] = repo_metrics.get("project_age_days")
                df.at[idx, "number_of_commits"] = repo_metrics.get("number_of_commits")
                df.at[idx, "total_lines_of_code"] = repo_metrics.get("total_lines_of_code")
                df.at[idx, "number_of_contributing_developers"] = repo_metrics.get("number_of_contributing_developers")
    
    df.rename(
        columns={
            "project_age_days": "github_project_age_days",
            "number_of_commits": "github_number_of_commits",
            "total_lines_of_code": "github_total_lines_of_code",
            "number_of_contributing_developers": "github_number_of_contributing_developers"
        },
        inplace=True
    )
    
    # Write the updated DataFrame to the specified output CSV file.
    print(f"Saving updated CSV file to: {output_file_path}")
    df.to_csv(output_file_path, index=False)
    print(f"Updated CSV file saved successfully to {output_file_path}")

if __name__ == "__main__":
    input_csv = "RDF/data/merged.csv"
    output_csv = "tested_merged.csv"
    github_token = os.environ.get('GITHUB_TOKEN')
    
    update_csv_with_metrics(input_csv, output_csv, origin_column="origin", token=github_token)
