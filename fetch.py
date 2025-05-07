import os
import csv
import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

@dataclass
class Comment:
    body: str
    created_at: datetime
    
@dataclass
class Review:
    state: str
    created_at: datetime
    body: str
    comment_count: int
    author: str
    comments: List[Comment]

@dataclass
class PRWithReviews:
    title: str
    url: str
    reviews: List[Review]
    total_comments: int
    
@dataclass
class TeamMemberSummary:
    name: str
    github_username: str
    authored_prs: int
    total_additions: int
    total_deletions: int
    total_files_changed: int
    reviews_given: int
    total_review_comments: int
    top_prs: List[Dict]  # Store top 10 most discussed PRs
    most_engaged_reviews: List[Dict]  # Store top 10 reviews with most comments
    all_prs: List[Dict]  # Store all PRs
    all_reviewed_prs: List[PRWithReviews]  # Store all reviewed PRs

import yaml
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.httpx import HTTPXTransport

# Load environment variables
load_dotenv()

# Constants
GITHUB_TOKEN = os.getenv("GITHUB_API_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_API_TOKEN environment variable is required")

# GraphQL queries
PR_QUERY = """
query ($searchQuery: String!, $after: String) {
  search(query: $searchQuery, type: ISSUE, first: 100, after: $after) {
    nodes {
      ... on PullRequest {
        id
        url
        title
        state
        createdAt
        updatedAt
        additions
        deletions
        changedFiles
        comments {
          totalCount
        }
        reviews {
          totalCount
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

REVIEWS_QUERY = """
query ($searchQuery: String!, $after: String, $reviewsAfter: String) {
  search(query: $searchQuery, type: ISSUE, first: 25, after: $after) {
    nodes {
      ... on PullRequest {
        number
        title
        url
        author {
          login
        }
        reviews(first: 100, after: $reviewsAfter) {
          totalCount
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            author {
              login
            }
            state
            createdAt
            body
            comments(first: 100) {
              totalCount
              nodes {
                body
                createdAt
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

def validate_github_token():
    """Validate the GitHub token has required permissions."""
    import httpx
    
    response = httpx.post(
        'https://api.github.com/graphql',
        headers={'Authorization': f'Bearer {GITHUB_TOKEN}'},
        json={'query': 'query { viewer { login } }'}
    )
    
    if response.status_code != 200:
        raise ValueError(f"GitHub token validation failed: {response.status_code} {response.text}")
    
    print(f"GitHub token validated successfully. Logged in as: {response.json()['data']['viewer']['login']}")

def create_github_client() -> Client:
    """Create an authenticated GitHub GraphQL client."""
    transport = HTTPXTransport(
        url='https://api.github.com/graphql',
        headers={'Authorization': f'Bearer {GITHUB_TOKEN}'},
        timeout=30.0  # Increase timeout to 30 seconds
    )
    
    # Validate token before creating client
    validate_github_token()
    
    return Client(transport=transport, fetch_schema_from_transport=True)

def load_team_members() -> List[Dict[str, str]]:
    """Load team members from team.yaml."""
    with open('team.yaml', 'r') as f:
        data = yaml.safe_load(f)
        return data.get('team', [])

def fetch_user_prs(client: Client, github_username: str) -> List[Dict]:
    """Fetch all PRs authored by a user since July 2024."""
    prs = []
    has_next_page = True
    cursor = None
    page = 1
    
    while has_next_page and page <= 5:  # Limit to 5 pages
        search_query = f"is:pr repo:amperity/app author:{github_username} created:>=2024-07-01"
        variables = {"searchQuery": search_query}
        if cursor:
            variables["after"] = cursor
        
        result = client.execute(gql(PR_QUERY), variable_values=variables)
        
        prs.extend(result['search']['nodes'])
        
        # Handle pagination
        page_info = result['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        page += 1
        
    return prs

def fetch_user_reviews(client: Client, github_username: str) -> List[PRWithReviews]:
    """Fetch all PR reviews authored by a user since July 2024."""
    reviews_by_pr = {}  # Dict[str, PRWithReviews]
    has_next_page = True
    cursor = None
    page = 1
    page_size = 25  # Reduce page size to handle memory better
    
    while has_next_page and page <= 10:  # Increase max pages but with smaller page size
        # Build the search query with pagination and narrow time window
        search_query = f"repo:amperity/app is:pr reviewed-by:{github_username} updated:>=2024-07-01 updated:<=2025-01-31"
        variables = {"searchQuery": search_query}
        if cursor:
            variables["after"] = cursor
        
        result = client.execute(gql(REVIEWS_QUERY), variable_values=variables)
        
        for pr in result['search']['nodes']:
            # Skip if this user authored the PR
            if pr['author']['login'] == github_username:
                continue
                
            # Handle pagination for reviews within each PR
            reviews_cursor = None
            has_more_reviews = True
            
            while has_more_reviews:
                if reviews_cursor:
                    # Fetch next page of reviews for this PR
                    variables = {
                        "searchQuery": search_query,
                        "after": cursor,
                        "reviewsAfter": reviews_cursor
                    }
                    result = client.execute(gql(REVIEWS_QUERY), variable_values=variables)
                    pr = result['search']['nodes'][0]  # We're querying the same PR
                
                # Get reviews by this author from current page
                author_reviews = [
                    review for review in pr['reviews']['nodes']
                    if review['author'] and review['author']['login'] == github_username
                ]
                
                if author_reviews:  # Only process if they reviewed someone else's PR
                    pr_url = pr['url']
                    if pr_url not in reviews_by_pr:
                        reviews_by_pr[pr_url] = PRWithReviews(
                            title=pr['title'],
                            url=pr_url,
                            reviews=[],
                            total_comments=0
                        )
                    
                    new_reviews = []
                    for review in author_reviews:
                        comments = [
                            Comment(
                                body=comment['body'],
                                created_at=datetime.fromisoformat(comment['createdAt'].replace('Z', '+00:00'))
                            )
                            for comment in review['comments']['nodes']
                        ]
                        
                        new_reviews.append(Review(
                            state=review['state'],
                            created_at=datetime.fromisoformat(review['createdAt'].replace('Z', '+00:00')),
                            body=review['body'],
                            comment_count=review['comments']['totalCount'],
                            author=github_username,
                            comments=comments
                        ))
                    
                    reviews_by_pr[pr_url].reviews.extend(new_reviews)
                    reviews_by_pr[pr_url].total_comments += sum(r.comment_count for r in new_reviews)
                
                # Check if we need to fetch more reviews
                reviews_page_info = pr['reviews']['pageInfo']
                has_more_reviews = reviews_page_info['hasNextPage']
                reviews_cursor = reviews_page_info['endCursor']
        
        # Handle pagination
        page_info = result['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        page += 1
        
    return list(reviews_by_pr.values())

def generate_member_summary(member: Dict, prs: List[Dict], reviews: List[PRWithReviews]) -> TeamMemberSummary:
    """Generate a summary of a team member's GitHub activity."""
    # Calculate PR statistics
    total_additions = sum(pr['additions'] for pr in prs)
    total_deletions = sum(pr['deletions'] for pr in prs)
    total_files = sum(pr['changedFiles'] for pr in prs)
    
    # Find top PRs by discussion volume (comments + reviews)
    top_prs = sorted(
        prs,
        key=lambda p: p['comments']['totalCount'] + p['reviews']['totalCount'],
        reverse=True
    )[:10]
    
    # Find most engaged reviews by comment count
    most_engaged = sorted(
        reviews,
        key=lambda r: r.total_comments,
        reverse=True
    )[:10]
    
    return TeamMemberSummary(
        name=member['name'],
        github_username=member['github'],
        authored_prs=len(prs),
        total_additions=total_additions,
        total_deletions=total_deletions,
        total_files_changed=total_files,
        reviews_given=sum(len(pr.reviews) for pr in reviews),
        total_review_comments=sum(pr.total_comments for pr in reviews),
        top_prs=top_prs,
        most_engaged_reviews=most_engaged,
        all_prs=prs,
        all_reviewed_prs=reviews
    )

def print_member_summary(summary: TeamMemberSummary):
    """Print a formatted summary for a team member."""
    print(f"\n=== Summary for {summary.name} ({summary.github_username}) ===")
    print(f"\nPR Activity:")
    print(f"- Authored {summary.authored_prs} PRs")
    print(f"- Changed {summary.total_files_changed} files (+{summary.total_additions}/-{summary.total_deletions})")
    print(f"- Gave {summary.reviews_given} reviews with {summary.total_review_comments} comments")
    
    print("\nTop 10 Most Discussed PRs Authored:")
    for pr in summary.top_prs:
        print(f"• {pr['title']}")
        print(f"  {pr['url']}")
        print(f"  {pr['comments']['totalCount']} comments, {pr['reviews']['totalCount']} reviews")
    
    print("\nTop 10 Most Engaged Reviews:")
    for pr in summary.most_engaged_reviews:
        print(f"• {pr.title}")
        print(f"  {pr.url}")
        print(f"  {pr.total_comments} comments across {len(pr.reviews)} reviews")
        # Print detailed review information
        for review in pr.reviews:
            print(f"    Review on {review.created_at.strftime('%Y-%m-%d %H:%M:%S')} - {review.state}")
            if review.body.strip():
                print(f"    Review comment: {review.body.strip()}")
            if review.comments:
                print("    Detailed comments:")
                for comment in review.comments:
                    print(f"      [{comment.created_at.strftime('%Y-%m-%d %H:%M:%S')}]")
                    print(f"      {comment.body.strip()}")
                print()
    
    print("\nAll Authored PRs:")
    for pr in summary.all_prs:
        print(f"• {pr['title']}")
        print(f"  {pr['url']}")
    
    print("\nAll Reviewed PRs with Comments:")
    for pr in summary.all_reviewed_prs:
        if any(review.comments or review.body.strip() for review in pr.reviews):
            print(f"\n• {pr.title}")
            print(f"  {pr.url}")
            for review in pr.reviews:
                if review.body.strip() or review.comments:
                    print(f"  Review on {review.created_at.strftime('%Y-%m-%d %H:%M:%S')} - {review.state}")
                    if review.body.strip():
                        print(f"    {review.body.strip()}")
                    for comment in review.comments:
                        print(f"    [{comment.created_at.strftime('%Y-%m-%d %H:%M:%S')}]")
                        print(f"    {comment.body.strip()}")

def export_reviews_to_csv(reviews: List[PRWithReviews], output_file: str):
    """Export review comments to CSV format."""
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow([
            'pr_url',
            'pr_title', 
            'review_date',
            'review_state',
            'review_body',
            'comment_date',
            'comment_body'
        ])
        
        # Write data
        for pr in reviews:
            for review in pr.reviews:
                # Write the review-level comment if it exists
                if review.body.strip():
                    writer.writerow([
                        pr.url,
                        pr.title,
                        review.created_at.isoformat(),
                        review.state,
                        review.body.strip(),
                        '',  # No specific comment date
                        ''   # No specific comment body
                    ])
                
                # Write individual comments
                for comment in review.comments:
                    writer.writerow([
                        pr.url,
                        pr.title,
                        review.created_at.isoformat(),
                        review.state,
                        '',  # No review body
                        comment.created_at.isoformat(),
                        comment.body.strip()
                    ])

def main():
    parser = argparse.ArgumentParser(
        description='Fetch and analyze GitHub contributions for team members',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get summary for all team members
  %(prog)s summary
  
  # Get summary for specific user
  %(prog)s summary --user johndoe
  
  # Export code review comments to CSV
  %(prog)s reviews --user johndoe --output reviews.csv
        """
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run', required=True)
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', 
        help='Generate activity summary',
        description='Generate a detailed summary of GitHub activity including PRs authored and reviews given.'
    )
    summary_parser.add_argument(
        '--user', 
        help='GitHub username to analyze (if omitted, analyzes all team members)',
        metavar='USERNAME'
    )
    
    # Reviews command
    reviews_parser = subparsers.add_parser('reviews',
        help='Export review comments to CSV',
        description='''Export code review comments to CSV format for analysis.
The CSV will include PR URLs, review states, comment text, and timestamps.'''
    )
    reviews_parser.add_argument(
        '--user',
        required=True,
        help='GitHub username whose reviews to analyze',
        metavar='USERNAME'
    )
    reviews_parser.add_argument(
        '--output',
        required=True,
        help='Path for output CSV file',
        metavar='FILE'
    )
    
    args = parser.parse_args()
    
    print("Initializing GitHub client...")
    client = create_github_client()
    print("GitHub client initialized successfully")
    
    # Verify repository exists
    test_query = """
    query {
      repository(owner: "amperity", name: "app") {
        name
      }
    }
    """
    try:
        result = client.execute(gql(test_query))
        print(f"Successfully connected to repository: {result['repository']['name']}")
    except Exception as e:
        print(f"Error verifying repository access: {e}")
        return
    team_members = load_team_members()
    
    if args.command == 'reviews':
        # Validate user exists in team
        member = next((m for m in team_members if m['github'] == args.user), None)
        if not member:
            print(f"Error: User {args.user} not found in team.yaml")
            return
            
        print(f"Fetching reviews for {args.user}...")
        reviews = fetch_user_reviews(client, args.user)
        print(f"Exporting {sum(len(pr.reviews) for pr in reviews)} reviews to {args.output}")
        export_reviews_to_csv(reviews, args.output)
        print("Export complete!")
        
    elif args.command == 'summary':
        # Filter to single user if specified
        if args.user:
            team_members = [m for m in team_members if m['github'] == args.user]
            if not team_members:
                print(f"Error: User {args.user} not found in team.yaml")
                return
        
        summaries = []
        for member in team_members:
            github_username = member['github']
            print(f"\nProcessing data for {member['name']} ({github_username})")
            
            prs = fetch_user_prs(client, github_username)
            reviews = fetch_user_reviews(client, github_username)
            
            summary = generate_member_summary(member, prs, reviews)
            summaries.append(summary)
            print_member_summary(summary)
        
        # Print team-wide statistics only for summary command
        print("\n=== Team-wide Statistics ===")
        print(f"Total PRs: {sum(s.authored_prs for s in summaries)}")
        print(f"Total Reviews: {sum(s.reviews_given for s in summaries)}")
        print(f"Total Files Changed: {sum(s.total_files_changed for s in summaries)}")

if __name__ == "__main__":
    main()
