#!/bin/bash

# Exit on error
set -e

# Print usage if incorrect input
usage() {
  echo "Usage: $0 -r <repo_path> -d <db_path> -t <gh_auth_token> -o <gh_project_owner> -n <gh_project_name>"
  exit 1
}

# Parse command line arguments
while getopts ":r:d:t:o:n:" opt; do
  case $opt in
    r) REPO_PATH="$OPTARG" ;;
    d) DB_PATH="$OPTARG" ;;
    t) GH_AUTH_TOKEN="$OPTARG" ;;
    o) GH_PROJECT_OWNER="$OPTARG" ;;
    n) GH_PROJECT_NAME="$OPTARG" ;;
    *) usage ;;
  esac
done

# Ensure all required arguments are provided
if [[ -z "$REPO_PATH" || -z "$DB_PATH" || -z "$GH_AUTH_TOKEN" || -z "$GH_PROJECT_OWNER" || -z "$GH_PROJECT_NAME" ]]; then
  usage
fi

# Run metrics
echo "Running VCS-related metrics..."
prime vcs --input "$REPO_PATH" --output "$DB_PATH" && \
prime filesize --input "$REPO_PATH" --output "$DB_PATH" && \
prime project-size --output "$DB_PATH" && \
prime project-productivity --output "$DB_PATH" && \
prime bus-factor --output "$DB_PATH"

echo "Running issue tracker metrics..."
prime issues --auth "$GH_AUTH_TOKEN" --owner "$GH_PROJECT_OWNER" --name "$GH_PROJECT_NAME" --output "$DB_PATH" && \
prime issue-spoilage --output "$DB_PATH" && \
prime issue-density --output "$DB_PATH"

echo "Running pull request metrics..."
prime pull-requests --auth "$GH_AUTH_TOKEN" --owner "$GH_PROJECT_OWNER" --name "$GH_PROJECT_NAME" --output "$DB_PATH" && \
prime pull-request-spoilage --output "$DB_PATH"
