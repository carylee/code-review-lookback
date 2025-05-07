# Code Review Lookback

A tool for analyzing GitHub pull request activity and code review patterns for team members.

## Overview

Code Review Lookback queries the GitHub GraphQL API to analyze:
- PRs authored by team members
- Code reviews and comments given by team members
- Statistics about code contribution and review engagement

Use this tool to understand team code review patterns, engagement levels, and to export code review feedback for further analysis.

## Installation

### Requirements
- Python 3.11+
- GitHub API token with repository access

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd code-review-lookback
```

2. Install dependencies:
```bash
pip install -e .
```

3. Create a `.env` file with your GitHub token:
```
GITHUB_API_TOKEN=your_token_here
```

4. Create a `team.yaml` file based on the example:
```yaml
team:
  - name: Full Name
    github: github_username
  - name: Another Name
    github: another_username
```

## Usage

### Generate Activity Summary

Generate a summary of GitHub activity for all team members:

```bash
python fetch.py summary
```

For a specific team member:

```bash
python fetch.py summary --user github_username
```

### Export Code Review Comments

Export code review comments to CSV for further analysis:

```bash
python fetch.py reviews --user github_username --output reviews.csv
```

### Customization Options

Customize behavior with these optional parameters:

```bash
# Specify a different repository
python fetch.py summary --repo owner/repo

# Set a custom date range
python fetch.py summary --start-date 2023-01-01 --end-date 2023-12-31

# Use a different team file
python fetch.py summary --team-file custom_team.yaml

# Enable verbose logging
python fetch.py summary -v
```

## Features

- **Activity Summary**: View statistics including PRs authored, files changed, and reviews given
- **Top PRs**: Lists most discussed PRs authored by team members
- **Most Engaged Reviews**: Shows PRs where team members gave the most detailed feedback
- **Team-wide Statistics**: Aggregates metrics across the team
- **CSV Export**: Export code review comments for analytical purposes

## Output

The summary output includes:

- Basic PR statistics (count, files changed, lines added/removed)
- Review activity (count, comments)  
- Top PRs by discussion volume
- Most engaged reviews with comment details
- List of all authored PRs and reviewed PRs
