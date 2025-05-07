import os
import csv
import logging
import argparse
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict, Union, Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Custom exceptions
class GitHubAPIError(Exception):
    """Exception raised for GitHub API errors."""
    pass

class RepositoryNotFoundError(GitHubAPIError):
    """Exception raised when a repository is not found."""
    pass

class TokenValidationError(GitHubAPIError):
    """Exception raised when GitHub token validation fails."""
    pass

class RateLimitError(GitHubAPIError):
    """Exception raised when GitHub API rate limit is reached."""
    pass

# Enums
class ReviewState(str, Enum):
    """GitHub PR review states."""
    APPROVED = "APPROVED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"
    COMMENTED = "COMMENTED"
    DISMISSED = "DISMISSED"
    PENDING = "PENDING"

# Data models with proper typing
@dataclass(frozen=True)
class Comment:
    """Represents a GitHub PR comment."""
    body: str
    created_at: datetime

@dataclass(frozen=True)
class Review:
    """Represents a GitHub PR review."""
    state: str
    created_at: datetime
    body: str
    comment_count: int
    author: str
    comments: List[Comment] = field(default_factory=list)

@dataclass
class PRWithReviews:
    """Represents a PR with its reviews."""
    title: str
    url: str
    reviews: List[Review] = field(default_factory=list)
    total_comments: int = 0

# TypedDict for PR data from API
class GitHubPR(TypedDict, total=False):
    """TypedDict for GitHub PR data."""
    id: str
    url: str
    title: str
    state: str
    createdAt: str
    updatedAt: str
    additions: int
    deletions: int
    changedFiles: int
    comments: Dict[str, int]
    reviews: Dict[str, int]

@dataclass
class TeamMemberSummary:
    """Summary of a team member's GitHub activity."""
    name: str
    github_username: str
    authored_prs: int = 0
    total_additions: int = 0
    total_deletions: int = 0
    total_files_changed: int = 0
    reviews_given: int = 0
    total_review_comments: int = 0
    top_prs: List[GitHubPR] = field(default_factory=list)  # Top 10 most discussed PRs
    most_engaged_reviews: List[PRWithReviews] = field(default_factory=list)  # Top 10 reviews with most comments
    all_prs: List[GitHubPR] = field(default_factory=list)  # All PRs
    all_reviewed_prs: List[PRWithReviews] = field(default_factory=list)  # All reviewed PRs

import yaml
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.httpx import HTTPXTransport
from gql.transport.exceptions import TransportQueryError
import httpx

def load_environment():
    """Load environment variables from .env file."""
    load_dotenv()
    github_token = os.getenv("GITHUB_API_TOKEN")
    if not github_token:
        raise TokenValidationError("GITHUB_API_TOKEN environment variable is required")
    return github_token

# Retry decorator for API calls
def retry_on_error(max_retries: int = 3, retry_delay: int = 2):
    """Decorator to retry functions on specific exceptions with exponential backoff."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except RateLimitError as e:
                    wait_time = retry_delay * (2 ** retries)
                    logger.warning(f"Rate limit hit. Retrying in {wait_time}s... ({retries+1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                except TransportQueryError as e:
                    if "rate limit" in str(e).lower():
                        wait_time = retry_delay * (2 ** retries)
                        logger.warning(f"Rate limit hit. Retrying in {wait_time}s... ({retries+1}/{max_retries})")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise GitHubAPIError(f"GitHub API error: {str(e)}")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 403 and "rate limit" in str(e).lower():
                        wait_time = retry_delay * (2 ** retries)
                        logger.warning(f"Rate limit hit. Retrying in {wait_time}s... ({retries+1}/{max_retries})")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise GitHubAPIError(f"HTTP error: {str(e)}")
            
            # If we've exhausted retries, raise the last exception
            raise GitHubAPIError(f"Failed after {max_retries} retries")
        return wrapper
    return decorator

# Load environment variables
try:
    GITHUB_TOKEN = load_environment()
except TokenValidationError as e:
    logger.error(str(e))
    raise

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

class GitHubClient:
    """Class for handling GitHub API interactions."""
    
    def __init__(self, token: str):
        """Initialize the GitHub client.
        
        Args:
            token: GitHub API token
        """
        self.token = token
        self.client = self._create_client()
        
    def _validate_token(self) -> str:
        """Validate the GitHub token has required permissions.
        
        Returns:
            str: GitHub username for the authenticated user
            
        Raises:
            TokenValidationError: If token validation fails
        """
        try:
            response = httpx.post(
                'https://api.github.com/graphql',
                headers={'Authorization': f'Bearer {self.token}'},
                json={'query': 'query { viewer { login } }'},
                timeout=30.0
            )
            
            if response.status_code != 200:
                raise TokenValidationError(
                    f"GitHub token validation failed: {response.status_code} {response.text}"
                )
                
            username = response.json()['data']['viewer']['login']
            logger.info(f"GitHub token validated successfully. Logged in as: {username}")
            return username
            
        except httpx.HTTPError as e:
            raise TokenValidationError(f"HTTP error during token validation: {str(e)}")
    
    def _create_client(self) -> Client:
        """Create an authenticated GitHub GraphQL client.
        
        Returns:
            Client: Configured GraphQL client
        """
        transport = HTTPXTransport(
            url='https://api.github.com/graphql',
            headers={'Authorization': f'Bearer {self.token}'},
            timeout=30.0
        )
        
        return Client(transport=transport, fetch_schema_from_transport=True)
    
    @retry_on_error()
    def validate_and_connect(self) -> str:
        """Validate token and ensure client is ready to use.
        
        Returns:
            str: GitHub username for the authenticated user
        """
        return self._validate_token()
    
    @retry_on_error()
    def verify_repository(self, owner: str, name: str) -> str:
        """Verify that a repository exists and is accessible.
        
        Args:
            owner: Repository owner
            name: Repository name
            
        Returns:
            str: Repository name if successful
            
        Raises:
            RepositoryNotFoundError: If repository doesn't exist or is not accessible
        """
        query = gql("""
        query ($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) {
            name
          }
        }
        """)
        
        try:
            result = self.client.execute(query, variable_values={"owner": owner, "name": name})
            repo_name = result['repository']['name']
            logger.info(f"Successfully connected to repository: {repo_name}")
            return repo_name
        except Exception as e:
            raise RepositoryNotFoundError(f"Error verifying repository access: {str(e)}")
    
    @retry_on_error()
    def execute_query(self, query, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a GraphQL query with variables.
        
        Args:
            query: GraphQL query
            variables: Query variables
            
        Returns:
            Dict: Query result
            
        Raises:
            GitHubAPIError: On query execution failure
        """
        try:
            return self.client.execute(query, variable_values=variables or {})
        except TransportQueryError as e:
            if "rate limit" in str(e).lower():
                raise RateLimitError(f"GitHub API rate limit exceeded: {str(e)}")
            raise GitHubAPIError(f"GraphQL query failed: {str(e)}")
        except Exception as e:
            raise GitHubAPIError(f"Error executing query: {str(e)}")

class TeamLoader:
    """Class for loading and validating team data."""
    
    @staticmethod
    def load_team_members(team_file: str = 'team.yaml') -> List[Dict[str, str]]:
        """Load team members from team file.
        
        Args:
            team_file: Path to team YAML file
            
        Returns:
            List[Dict[str, str]]: List of team members with their info
            
        Raises:
            FileNotFoundError: If team file doesn't exist
            ValueError: If team file has invalid format
        """
        try:
            with open(team_file, 'r') as f:
                data = yaml.safe_load(f)
                if not isinstance(data, dict) or 'team' not in data:
                    raise ValueError(f"Invalid team file format in {team_file}. Expected 'team' key with list value.")
                
                team = data.get('team', [])
                if not isinstance(team, list):
                    raise ValueError(f"Invalid team data in {team_file}. Expected list of team members.")
                    
                # Validate team members have required fields
                for i, member in enumerate(team):
                    if not isinstance(member, dict):
                        raise ValueError(f"Invalid team member format at index {i}. Expected dict.")
                    if 'name' not in member:
                        raise ValueError(f"Missing 'name' for team member at index {i}")
                    if 'github' not in member:
                        raise ValueError(f"Missing 'github' username for team member {member.get('name', f'at index {i}')}")
                
                logger.info(f"Loaded {len(team)} team members from {team_file}")
                return team
                
        except FileNotFoundError:
            logger.error(f"Team file not found: {team_file}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing team file {team_file}: {str(e)}")
            raise ValueError(f"Invalid YAML in team file: {str(e)}")
    
    @staticmethod
    def get_team_member(team_members: List[Dict[str, str]], github_username: str) -> Dict[str, str]:
        """Get a team member by GitHub username.
        
        Args:
            team_members: List of team members
            github_username: GitHub username to look for
            
        Returns:
            Dict[str, str]: Team member info
            
        Raises:
            ValueError: If team member not found
        """
        member = next((m for m in team_members if m['github'] == github_username), None)
        if not member:
            raise ValueError(f"User {github_username} not found in team data")
        return member

class GitHubDataFetcher:
    """Class for fetching and processing GitHub data."""
    
    def __init__(self, github_client: GitHubClient):
        """Initialize with a GitHub client.
        
        Args:
            github_client: Authenticated GitHub client
        """
        self.github_client = github_client
        # Compile GraphQL queries
        self.pr_query = gql(PR_QUERY)
        self.reviews_query = gql(REVIEWS_QUERY)
    
    def _build_date_filter(self, field: str, start_date: str, end_date: Optional[str] = None) -> str:
        """Build a date filter string for GitHub search queries.
        
        Args:
            field: Field name for the date filter (e.g., 'created', 'updated')
            start_date: Start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            
        Returns:
            str: Formatted date filter string
        """
        date_filter = f"{field}:>={start_date}"
        if end_date:
            date_filter += f" {field}:<={end_date}"
        return date_filter
    
    def _paginate_results(self, query_func: Callable, max_pages: int = 5) -> List[Dict]:
        """Generic pagination handler for GitHub GraphQL queries.
        
        Args:
            query_func: Function that takes cursor and returns (items, has_next, cursor)
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List[Dict]: Aggregated results from all pages
        """
        results = []
        has_next_page = True
        cursor = None
        page = 1
        
        while has_next_page and page <= max_pages:
            items, has_next_page, cursor = query_func(cursor)
            results.extend(items)
            page += 1
            if page % 2 == 0:  # Log progress every 2 pages
                logger.info(f"Fetched {len(results)} items ({page-1}/{max_pages} pages)")
        
        return results
    
    def fetch_user_prs(self, github_username: str, repository: str = 'amperity/app', 
                     start_date: str = '2024-07-01', end_date: str = None) -> List[GitHubPR]:
        """Fetch all PRs authored by a user within a date range.
        
        Args:
            github_username: GitHub username to fetch PRs for
            repository: Repository in format 'owner/repo'
            start_date: Start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            
        Returns:
            List[GitHubPR]: List of pull requests
        """
        logger.info(f"Fetching PRs authored by {github_username} in {repository} since {start_date}")
        
        # Build date filter
        date_filter = self._build_date_filter('created', start_date, end_date)
        search_query = f"is:pr repo:{repository} author:{github_username} {date_filter}"
        
        def query_page(cursor):
            variables = {"searchQuery": search_query}
            if cursor:
                variables["after"] = cursor
            
            result = self.github_client.execute_query(self.pr_query, variables)
            
            nodes = result['search']['nodes']
            page_info = result['search']['pageInfo']
            return nodes, page_info['hasNextPage'], page_info['endCursor']
        
        prs = self._paginate_results(query_page, max_pages=5)
        logger.info(f"Fetched {len(prs)} PRs for {github_username}")
        return prs

    def fetch_user_reviews(self, github_username: str, repository: str = 'amperity/app', 
                        start_date: str = '2024-07-01', end_date: str = '2025-01-31') -> List[PRWithReviews]:
        """Fetch all PR reviews authored by a user within a date range.
        
        Args:
            github_username: GitHub username to fetch reviews for
            repository: Repository in format 'owner/repo'
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List[PRWithReviews]: List of PRs with reviews
        """
        logger.info(f"Fetching reviews by {github_username} in {repository} from {start_date} to {end_date}")
        
        reviews_by_pr = {}  # Dict[str, PRWithReviews]
        
        # Build date filter
        date_filter = self._build_date_filter('updated', start_date, end_date)
        search_query = f"repo:{repository} is:pr reviewed-by:{github_username} {date_filter}"
        
        def query_page(cursor):
            variables = {"searchQuery": search_query}
            if cursor:
                variables["after"] = cursor
            
            result = self.github_client.execute_query(self.reviews_query, variables)
            
            # Process PRs from this page
            for pr in result['search']['nodes']:
                self._process_pr_reviews(pr, github_username, search_query, cursor, reviews_by_pr)
            
            page_info = result['search']['pageInfo']
            return result['search']['nodes'], page_info['hasNextPage'], page_info['endCursor']
        
        # Paginate through all matching PRs
        self._paginate_results(query_page, max_pages=10)
        
        logger.info(f"Fetched reviews from {len(reviews_by_pr)} PRs for {github_username}")
        return list(reviews_by_pr.values())
    
    def _process_pr_reviews(self, pr: Dict, github_username: str, search_query: str, 
                         cursor: Optional[str], reviews_by_pr: Dict[str, PRWithReviews]) -> None:
        """Process reviews for a single PR.
        
        Args:
            pr: PR data from GitHub API
            github_username: GitHub username to filter reviews by
            search_query: Original search query
            cursor: Current pagination cursor
            reviews_by_pr: Dictionary to store processed reviews
        """
        # Skip if this user authored the PR
        if pr['author']['login'] == github_username:
            return
            
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
                result = self.github_client.execute_query(self.reviews_query, variables)
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
                        url=pr_url
                    )
                
                # Process each review
                for review in author_reviews:
                    self._add_review_to_pr(review, github_username, reviews_by_pr[pr_url])
            
            # Check if we need to fetch more reviews
            reviews_page_info = pr['reviews']['pageInfo']
            has_more_reviews = reviews_page_info['hasNextPage']
            reviews_cursor = reviews_page_info['endCursor']
    
    def _add_review_to_pr(self, review: Dict, github_username: str, pr_with_reviews: PRWithReviews) -> None:
        """Process a single review and add it to a PRWithReviews object.
        
        Args:
            review: Review data from GitHub API
            github_username: GitHub username of reviewer
            pr_with_reviews: PRWithReviews object to add review to
        """
        # Convert comments to our model
        comments = []
        for comment in review['comments']['nodes']:
            try:
                created_at = datetime.fromisoformat(comment['createdAt'].replace('Z', '+00:00'))
                comments.append(Comment(
                    body=comment['body'],
                    created_at=created_at
                ))
            except (ValueError, KeyError) as e:
                logger.warning(f"Error processing comment: {str(e)}")
        
        # Create review object
        try:
            created_at = datetime.fromisoformat(review['createdAt'].replace('Z', '+00:00'))
            review_obj = Review(
                state=review['state'],
                created_at=created_at,
                body=review['body'],
                comment_count=review['comments']['totalCount'],
                author=github_username,
                comments=comments
            )
            
            # Add to PR
            pr_with_reviews.reviews.append(review_obj)
            pr_with_reviews.total_comments += review_obj.comment_count
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Error processing review: {str(e)}")

class ActivityAnalyzer:
    """Class for analyzing GitHub activity data."""
    
    @staticmethod
    def generate_member_summary(member: Dict[str, str], 
                             prs: List[GitHubPR], 
                             reviews: List[PRWithReviews]) -> TeamMemberSummary:
        """Generate a summary of a team member's GitHub activity.
        
        Args:
            member: Team member information
            prs: PRs authored by the team member
            reviews: PRs reviewed by the team member
            
        Returns:
            TeamMemberSummary: Summary of the team member's activity
        """
        logger.info(f"Generating summary for {member['name']} ({member['github']})")
        
        try:
            # Calculate PR statistics - handle missing fields gracefully
            total_additions = sum(pr.get('additions', 0) for pr in prs)
            total_deletions = sum(pr.get('deletions', 0) for pr in prs)
            total_files = sum(pr.get('changedFiles', 0) for pr in prs)
            
            # Find top PRs by discussion volume (comments + reviews)
            top_prs = sorted(
                prs,
                key=lambda p: (
                    p.get('comments', {}).get('totalCount', 0) + 
                    p.get('reviews', {}).get('totalCount', 0)
                ),
                reverse=True
            )[:10]
            
            # Find most engaged reviews by comment count
            most_engaged = sorted(
                reviews,
                key=lambda r: r.total_comments,
                reverse=True
            )[:10]
            
            # Create summary
            summary = TeamMemberSummary(
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
            
            logger.info(f"Summary generated for {member['name']}: {summary.authored_prs} PRs, {summary.reviews_given} reviews")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary for {member['name']}: {str(e)}")
            # Return a partial summary with what we have
            return TeamMemberSummary(
                name=member['name'],
                github_username=member['github'],
                all_prs=prs,
                all_reviewed_prs=reviews
            )

class ReportFormatter:
    """Class for formatting and displaying GitHub activity reports."""
    
    @staticmethod
    def format_date(dt: datetime) -> str:
        """Format a datetime object for display.
        
        Args:
            dt: Datetime to format
            
        Returns:
            str: Formatted date string
        """
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def print_member_summary(summary: TeamMemberSummary) -> None:
        """Print a formatted summary for a team member.
        
        Args:
            summary: Team member summary to print
        """
        logger.info(f"Displaying summary for {summary.name}")
        
        print(f"\n=== Summary for {summary.name} ({summary.github_username}) ===")
        print(f"\nPR Activity:")
        print(f"- Authored {summary.authored_prs} PRs")
        print(f"- Changed {summary.total_files_changed} files (+{summary.total_additions}/-{summary.total_deletions})")
        print(f"- Gave {summary.reviews_given} reviews with {summary.total_review_comments} comments")
        
        ReportFormatter._print_top_prs(summary.top_prs)
        ReportFormatter._print_engaged_reviews(summary.most_engaged_reviews)
        ReportFormatter._print_all_prs(summary.all_prs)
        ReportFormatter._print_all_reviews(summary.all_reviewed_prs)
    
    @staticmethod
    def _print_top_prs(prs: List[GitHubPR]) -> None:
        """Print top PRs by discussion volume.
        
        Args:
            prs: List of PRs to print
        """
        print("\nTop 10 Most Discussed PRs Authored:")
        if not prs:
            print("  None found")
            return
            
        for pr in prs:
            print(f"• {pr.get('title', 'No title')}")
            print(f"  {pr.get('url', 'No URL')}")
            comments_count = pr.get('comments', {}).get('totalCount', 0)
            reviews_count = pr.get('reviews', {}).get('totalCount', 0)
            print(f"  {comments_count} comments, {reviews_count} reviews")
    
    @staticmethod
    def _print_engaged_reviews(reviews: List[PRWithReviews]) -> None:
        """Print most engaged reviews.
        
        Args:
            reviews: List of reviews to print
        """
        print("\nTop 10 Most Engaged Reviews:")
        if not reviews:
            print("  None found")
            return
            
        for pr in reviews:
            print(f"• {pr.title}")
            print(f"  {pr.url}")
            print(f"  {pr.total_comments} comments across {len(pr.reviews)} reviews")
            
            # Print detailed review information
            for review in pr.reviews:
                print(f"    Review on {ReportFormatter.format_date(review.created_at)} - {review.state}")
                if review.body.strip():
                    print(f"    Review comment: {review.body.strip()}")
                if review.comments:
                    print("    Detailed comments:")
                    for comment in review.comments:
                        print(f"      [{ReportFormatter.format_date(comment.created_at)}]")
                        print(f"      {comment.body.strip()}")
                    print()
    
    @staticmethod
    def _print_all_prs(prs: List[GitHubPR]) -> None:
        """Print all authored PRs.
        
        Args:
            prs: List of PRs to print
        """
        print("\nAll Authored PRs:")
        if not prs:
            print("  None found")
            return
            
        for pr in prs:
            print(f"• {pr.get('title', 'No title')}")
            print(f"  {pr.get('url', 'No URL')}")
    
    @staticmethod
    def _print_all_reviews(reviews: List[PRWithReviews]) -> None:
        """Print all reviewed PRs with comments.
        
        Args:
            reviews: List of reviews to print
        """
        print("\nAll Reviewed PRs with Comments:")
        if not reviews:
            print("  None found")
            return
            
        has_comments = False
        for pr in reviews:
            if any(review.comments or review.body.strip() for review in pr.reviews):
                has_comments = True
                print(f"\n• {pr.title}")
                print(f"  {pr.url}")
                for review in pr.reviews:
                    if review.body.strip() or review.comments:
                        print(f"  Review on {ReportFormatter.format_date(review.created_at)} - {review.state}")
                        if review.body.strip():
                            print(f"    {review.body.strip()}")
                        for comment in review.comments:
                            print(f"    [{ReportFormatter.format_date(comment.created_at)}]")
                            print(f"    {comment.body.strip()}")
        
        if not has_comments:
            print("  None found")

class DataExporter:
    """Class for exporting GitHub data to various formats."""
    
    @staticmethod
    def export_reviews_to_csv(reviews: List[PRWithReviews], output_file: str) -> None:
        """Export review comments to CSV format.
        
        Args:
            reviews: List of PRs with review data
            output_file: Path to output CSV file
            
        Raises:
            IOError: If file cannot be written
        """
        logger.info(f"Exporting {sum(len(pr.reviews) for pr in reviews)} reviews to {output_file}")
        
        try:
            # Create directory if it doesn't exist
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
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
                rows_written = 0
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
                            rows_written += 1
                        
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
                            rows_written += 1
                
                logger.info(f"Exported {rows_written} rows to {output_file}")
                
        except IOError as e:
            logger.error(f"Error writing to {output_file}: {str(e)}")
            raise

class CLIParser:
    """Command Line Interface parser configuration."""
    
    @staticmethod
    def add_common_arguments(parser: argparse.ArgumentParser) -> None:
        """Add common arguments to a parser.
        
        Args:
            parser: ArgumentParser to add arguments to
        """
        parser.add_argument(
            '--repo',
            default='amperity/app',
            help='GitHub repository in format "owner/repo"',
            metavar='OWNER/REPO'
        )
        parser.add_argument(
            '--start-date',
            default='2024-07-01',
            help='Start date for data collection (YYYY-MM-DD)',
            metavar='DATE'
        )
        parser.add_argument(
            '--end-date',
            default='2025-01-31',
            help='End date for data collection (YYYY-MM-DD)',
            metavar='DATE'
        )
        parser.add_argument(
            '--team-file',
            default='team.yaml',
            help='Path to team members YAML file',
            metavar='FILE'
        )
        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='Enable verbose logging'
        )

class CodeReviewAnalyzer:
    """Main application class for code review analysis."""
    
    def __init__(self):
        """Initialize the analyzer."""
        self.github_client = None
        self.data_fetcher = None
    
    def configure_logging(self, verbose: bool = False) -> None:
        """Configure logging level based on verbosity.
        
        Args:
            verbose: Whether to enable verbose logging
        """
        if verbose:
            logger.setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")
        else:
            logger.setLevel(logging.INFO)
    
    def setup_github_client(self) -> None:
        """Set up the GitHub client.
        
        Raises:
            GitHubAPIError: If GitHub client setup fails
        """
        logger.info("Initializing GitHub client...")
        self.github_client = GitHubClient(GITHUB_TOKEN)
        self.github_client.validate_and_connect()
        self.data_fetcher = GitHubDataFetcher(self.github_client)
        logger.info("GitHub client initialized successfully")
    
    def validate_repository(self, repo_string: str) -> tuple:
        """Validate and parse repository string.
        
        Args:
            repo_string: Repository string in format "owner/repo"
            
        Returns:
            tuple: (owner, repo_name)
            
        Raises:
            ValueError: If repository format is invalid
        """
        repo_parts = repo_string.split('/')
        if len(repo_parts) != 2:
            raise ValueError(f"Invalid repository format. Expected 'owner/repo', got '{repo_string}'")
        
        owner, repo = repo_parts
        self.github_client.verify_repository(owner, repo)
        return owner, repo
    
    def process_reviews_command(self, args) -> None:
        """Process the reviews command.
        
        Args:
            args: Command line arguments
            
        Raises:
            ValueError: If user not found or processing fails
        """
        # Load team members
        team_members = TeamLoader.load_team_members(args.team_file)
        
        # Validate user exists in team
        try:
            member = TeamLoader.get_team_member(team_members, args.user)
            logger.info(f"Processing reviews for {member['name']} ({args.user})")
        except ValueError as e:
            logger.error(str(e))
            raise
        
        # Fetch and export reviews
        logger.info(f"Fetching reviews for {args.user}...")
        reviews = self.data_fetcher.fetch_user_reviews(
            args.user, 
            repository=args.repo,
            start_date=args.start_date, 
            end_date=args.end_date
        )
        
        DataExporter.export_reviews_to_csv(reviews, args.output)
        logger.info("Export complete!")
    
    def process_summary_command(self, args) -> None:
        """Process the summary command.
        
        Args:
            args: Command line arguments
            
        Raises:
            ValueError: If user not found or processing fails
        """
        # Load team members
        team_members = TeamLoader.load_team_members(args.team_file)
        
        # Filter to single user if specified
        if args.user:
            try:
                member = TeamLoader.get_team_member(team_members, args.user)
                team_members = [member]
            except ValueError as e:
                logger.error(str(e))
                raise
        
        # Process each team member
        summaries = []
        for member in team_members:
            github_username = member['github']
            logger.info(f"Processing data for {member['name']} ({github_username})")
            
            # Fetch data
            prs = self.data_fetcher.fetch_user_prs(
                github_username, 
                repository=args.repo,
                start_date=args.start_date, 
                end_date=args.end_date
            )
            
            reviews = self.data_fetcher.fetch_user_reviews(
                github_username, 
                repository=args.repo,
                start_date=args.start_date, 
                end_date=args.end_date
            )
            
            # Generate and display summary
            summary = ActivityAnalyzer.generate_member_summary(member, prs, reviews)
            summaries.append(summary)
            ReportFormatter.print_member_summary(summary)
        
        # Print team-wide statistics
        if summaries:
            logger.info("Generating team-wide statistics")
            print("\n=== Team-wide Statistics ===")
            print(f"Total PRs: {sum(s.authored_prs for s in summaries)}")
            print(f"Total Reviews: {sum(s.reviews_given for s in summaries)}")
            print(f"Total Files Changed: {sum(s.total_files_changed for s in summaries)}")
    
    def main(self) -> int:
        """Main entry point.
        
        Returns:
            int: Exit code (0 for success, non-zero for error)
        """
        # Set up argument parser
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
  
  # Specify custom repository and date range
  %(prog)s summary --repo owner/repo --start-date 2023-01-01 --end-date 2023-12-31
  
  # Enable verbose logging
  %(prog)s summary -v
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
        CLIParser.add_common_arguments(summary_parser)
        
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
        CLIParser.add_common_arguments(reviews_parser)
        
        # Parse arguments
        args = parser.parse_args()
        
        # Configure logging
        self.configure_logging(args.verbose)
        
        try:
            # Set up GitHub client
            self.setup_github_client()
            
            # Validate repository format
            self.validate_repository(args.repo)
            
            # Process command
            if args.command == 'reviews':
                self.process_reviews_command(args)
            elif args.command == 'summary':
                self.process_summary_command(args)
            
            return 0
        
        except TokenValidationError as e:
            logger.error(f"GitHub token error: {str(e)}")
            return 1
        except RepositoryNotFoundError as e:
            logger.error(f"Repository error: {str(e)}")
            return 1
        except ValueError as e:
            logger.error(str(e))
            return 1
        except GitHubAPIError as e:
            logger.error(f"GitHub API error: {str(e)}")
            return 1
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            if args.verbose:
                logger.exception("Detailed error information:")
            return 1

if __name__ == "__main__":
    analyzer = CodeReviewAnalyzer()
    exit(analyzer.main())
