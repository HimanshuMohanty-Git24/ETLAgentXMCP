"""
GitHub Tools for PR creation and version control operations.

Provides tools for creating branches, committing code, creating PRs,
and checking PR status using GitHub REST API and GitPython.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from typing import Dict, Any, Optional
import requests
from git import Repo
from langchain_core.tools import tool
from datetime import datetime
import base64


# GitHub configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME")
GIT_LOCAL_PATH = os.getenv("GIT_LOCAL_PATH", "./repo")

GITHUB_API_BASE = "https://api.github.com"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}


@tool
def create_branch(branch_name: str, base_branch: str = "main") -> Dict[str, Any]:
    """
    Create a new Git branch.
    
    Args:
        branch_name: Name of the new branch
        base_branch: Base branch to branch from (default: main)
        
    Returns:
        Dictionary with branch creation result
    """
    try:
        # Get base branch SHA
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/git/ref/heads/{base_branch}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        base_sha = response.json()["object"]["sha"]
        
        # Create new branch
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/git/refs"
        data = {
            "ref": f"refs/heads/{branch_name}",
            "sha": base_sha
        }
        
        response = requests.post(url, headers=HEADERS, json=data)
        
        if response.status_code == 201:
            return {
                "status": "success",
                "branch_name": branch_name,
                "base_branch": base_branch,
                "sha": base_sha
            }
        else:
            return {
                "status": "error",
                "error": response.json().get("message", "Unknown error"),
                "branch_name": branch_name
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "branch_name": branch_name
        }


@tool
def commit_and_push(
    branch_name: str,
    file_path: str,
    file_content: str,
    commit_message: str
) -> Dict[str, Any]:
    """
    Commit a file to a branch using GitHub API.
    
    Args:
        branch_name: Branch to commit to
        file_path: Path of the file in the repository
        file_content: Content of the file
        commit_message: Commit message
        
    Returns:
        Dictionary with commit result
    """
    try:
        # Check if file exists
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{file_path}"
        params = {"ref": branch_name}
        response = requests.get(url, headers=HEADERS, params=params)
        
        sha = None
        if response.status_code == 200:
            sha = response.json()["sha"]
        
        # Create or update file
        content_b64 = base64.b64encode(file_content.encode()).decode()
        data = {
            "message": commit_message,
            "content": content_b64,
            "branch": branch_name
        }
        
        if sha:
            data["sha"] = sha
        
        response = requests.put(url, headers=HEADERS, json=data)
        response.raise_for_status()
        
        return {
            "status": "success",
            "branch_name": branch_name,
            "file_path": file_path,
            "commit_sha": response.json()["commit"]["sha"],
            "commit_message": commit_message
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "branch_name": branch_name,
            "file_path": file_path
        }


@tool
def create_github_pr(
    title: str,
    body: str,
    head_branch: str,
    base_branch: str = "main"
) -> Dict[str, Any]:
    """
    Create a GitHub Pull Request.
    
    Args:
        title: PR title
        body: PR description body
        head_branch: Source branch for the PR
        base_branch: Target branch (default: main)
        
    Returns:
        Dictionary with PR creation result including PR number and URL
    """
    try:
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/pulls"
        data = {
            "title": title,
            "body": body,
            "head": head_branch,
            "base": base_branch
        }
        
        response = requests.post(url, headers=HEADERS, json=data)
        response.raise_for_status()
        
        pr_data = response.json()
        
        return {
            "status": "success",
            "pr_number": pr_data["number"],
            "pr_url": pr_data["html_url"],
            "branch_name": head_branch,
            "base_branch": base_branch,
            "title": title,
            "state": pr_data["state"],
            "created_at": pr_data["created_at"]
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "head_branch": head_branch,
            "base_branch": base_branch
        }


@tool
def check_pr_status(pr_number: int) -> Dict[str, Any]:
    """
    Check the status of a GitHub Pull Request.
    
    Args:
        pr_number: PR number to check
        
    Returns:
        Dictionary with PR status information
    """
    try:
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/pulls/{pr_number}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        pr_data = response.json()
        
        return {
            "status": "success",
            "pr_number": pr_number,
            "state": pr_data["state"],
            "merged": pr_data.get("merged", False),
            "mergeable": pr_data.get("mergeable"),
            "url": pr_data["html_url"],
            "title": pr_data["title"],
            "created_at": pr_data["created_at"],
            "updated_at": pr_data["updated_at"],
            "merged_at": pr_data.get("merged_at"),
            "head_branch": pr_data["head"]["ref"],
            "base_branch": pr_data["base"]["ref"]
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "pr_number": pr_number
        }


@tool
def merge_pr(pr_number: int, commit_title: Optional[str] = None) -> Dict[str, Any]:
    """
    Merge a GitHub Pull Request.
    
    Args:
        pr_number: PR number to merge
        commit_title: Optional commit title for merge
        
    Returns:
        Dictionary with merge result
    """
    try:
        url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/pulls/{pr_number}/merge"
        data = {}
        
        if commit_title:
            data["commit_title"] = commit_title
        
        response = requests.put(url, headers=HEADERS, json=data)
        response.raise_for_status()
        
        merge_data = response.json()
        
        return {
            "status": "success",
            "pr_number": pr_number,
            "merged": merge_data.get("merged", False),
            "sha": merge_data.get("sha"),
            "message": merge_data.get("message")
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "pr_number": pr_number
        }
