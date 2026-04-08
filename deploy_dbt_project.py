#!/usr/bin/env python3
"""Deploy dbt project to Databricks Repos.

Standalone deployment script for the generated dbt project.
Usage:
    python deploy_dbt_project.py --workspace-url https://<workspace>.azuredatabricks.net \\
        --token <PAT> --git-url https://github.com/org/repo --branch main
"""
import argparse
import json
import os
import sys

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)


def deploy_to_repos(workspace_url, token, repo_path, git_url, branch="main"):
    """Create or update a Databricks Repo linked to the dbt project."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Try to create the repo
    resp = requests.post(
        f"{workspace_url.rstrip('/')}/api/2.0/repos",
        headers=headers,
        json={"url": git_url, "provider": "github", "path": repo_path},
        timeout=30,
    )

    if resp.status_code == 200:
        print(f"  Created repo: {repo_path}")
        return resp.json()
    elif resp.status_code == 400 and "already exists" in resp.text.lower():
        print(f"  Repo already exists: {repo_path}")
        # Update to latest branch
        list_resp = requests.get(
            f"{workspace_url.rstrip('/')}/api/2.0/repos",
            headers=headers,
            params={"path_prefix": repo_path},
            timeout=30,
        )
        if list_resp.ok:
            repos = list_resp.json().get("repos", [])
            if repos:
                repo_id = repos[0]["id"]
                update_resp = requests.patch(
                    f"{workspace_url.rstrip('/')}/api/2.0/repos/{repo_id}",
                    headers=headers,
                    json={"branch": branch},
                    timeout=30,
                )
                if update_resp.ok:
                    print(f"  Updated to branch: {branch}")
                    return update_resp.json()
        return {"status": "exists"}
    else:
        print(f"  Error: {resp.status_code} {resp.text[:200]}")
        return {"error": resp.status_code}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy dbt project to Databricks")
    parser.add_argument("--workspace-url", required=True,
                        help="Databricks workspace URL (https://<workspace>.azuredatabricks.net)")
    parser.add_argument("--token", default=None,
                        help="Databricks personal access token (prefer DATABRICKS_TOKEN env var)")
    parser.add_argument("--repo-path", default="/Repos/migration/dbt_project",
                        help="Target path in Databricks Repos")
    parser.add_argument("--git-url", required=True,
                        help="Git repository URL containing the dbt project")
    parser.add_argument("--branch", default="main",
                        help="Git branch to deploy (default: main)")
    args = parser.parse_args()

    token = args.token or os.environ.get("DATABRICKS_TOKEN")
    if not token:
        print("ERROR: Provide --token or set DATABRICKS_TOKEN environment variable")
        sys.exit(1)

    result = deploy_to_repos(
        args.workspace_url, token, args.repo_path, args.git_url, args.branch
    )
    print(json.dumps(result, indent=2))
