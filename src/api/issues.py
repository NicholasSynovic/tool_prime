"""
Issue tracker interfaces and classes.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from requests import Response, post
from pandas import DataFrame
from string import Template
from functools import partial
from operator import itemgetter
from src.api import VALID_RESPONSE_CODE


def query_graphql(
    url: str,
    json_query: str,
    headers: dict[str, str],
    timeout: int = 60,
) -> Response:
    """
    Send a GraphQL query to the specified endpoint.

    Executes a POST request to the given GraphQL URL with the specified query,
    headers, and optional timeout. Returns the HTTP response.

    Args:
        url (str): The GraphQL endpoint URL.
        json_query (str): The GraphQL query string to send.
        headers (dict[str, str]): A dictionary of HTTP headers to include in the
            request.
        timeout (int, optional): Timeout for the request in seconds. Defaults to 60.

    Returns:
        Response: The HTTP response object from the request.

    """
    return post(
        url=url,
        json={"query": json_query},
        headers=headers,
        timeout=timeout,
    )


class GitHubIssues:
    """
    Class to interact with the GitHub GraphQL API to fetch issue metadata.

    Attributes:
        base_url (str): The GitHub GraphQL API endpoint.
        owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        auth_key (str): GitHub personal access token for authentication.
        headers (dict[str, str]): HTTP headers including authorization for API
            requests.
        query_function (Callable): A partially applied function for executing
            GraphQL queries.

    Methods:
        get_total_issues() -> int:
            Retrieves the total number of issues in the repository.

        get_issues(after_cursor: str = "null") -> tuple[pd.DataFrame, str, bool]:
            Retrieves paginated issue metadata from the repository.

    """

    def __init__(self, owner: str, repo_name: str, auth_key: str) -> None:
        """
        Initialize the GitHubIssues instance.

        Args:
            owner (str): The owner of the GitHub repository.
            repo_name (str): The name of the GitHub repository.
            auth_key (str): A GitHub personal access token for authenticating API
                requests.

        """
        self.base_url: str = "https://api.github.com/graphql"
        self.owner: str = owner
        self.repo_name: str = repo_name
        self.auth_key: str = auth_key
        self.headers: dict[str, str] = {
            "Authorization": f"Bearer {self.auth_key}",
            "Content-Type": "application/json",
        }
        self.query_function: partial = partial(
            query_graphql,
            url=self.base_url,
            headers=self.headers,
        )

    def get_total_issues(self) -> int:
        """
        Query the GitHub GraphQL API to retrieve the total number of issues.

        Returns:
            int: The total count of issues if the request is successful;
                -1 if the request fails (e.g., due to a non-200 status code).

        """
        json_template: Template = Template(
            template="""
            query {
                repository(name:"$name", owner:"$owner") {
                    issues {
                        totalCount
                    }
                },
                rateLimit {
                    limit,
                    cost,
                    remaining,
                    resetAt
                }
            }"""
        )
        json_query = json_template.substitute(
            name=self.repo_name,
            owner=self.owner,
        )

        response: Response = self.query_function(json_query=json_query)

        if response.status_code != VALID_RESPONSE_CODE:
            return -1

        return response.json()["data"]["repository"]["issues"]["totalCount"]

    def get_issues(self, after_cursor: str = "null") -> tuple[DataFrame, str, bool]:
        """
        Retrieve a page of GitHub issues from a repository using GraphQL.

        Constructs and sends a GraphQL query to retrieve up to 100 issues from
        the specified repository, starting after the provided cursor. Returns
        the issues in a DataFrame, along with the cursor for the next page and a
        boolean indicating if more pages exist.

        Args:
            after_cursor (str, optional): The pagination cursor to continue fetching
                issues from. Use "null" to start from the beginning.
                Defaults to "null".

        Returns:
            tuple[DataFrame, str, bool]:
                - A DataFrame containing issue data (`id`, `createdAt`, `closedAt`).
                - A string representing the end cursor for pagination.
                - A boolean indicating whether there are more pages of issues to fetch.

        """
        json_template: Template = Template(
            template="""query {
                repository(name:"$name", owner:"$owner") {
                    issues(first: 100, after: $cursor) {
                        edges {
                            node {
                                id,
                                createdAt,
                                closedAt,
                            }
                        },
                        pageInfo {
                            endCursor,
                            hasNextPage
                        }
                    }
                },
                rateLimit {
                    limit,
                    cost,
                    remaining,
                    resetAt
                }
            }"""
        )

        json_query = json_template.substitute(
            name=self.repo_name,
            owner=self.owner,
            cursor=after_cursor,
        )

        response: Response = self.query_function(json_query=json_query)

        if response.status_code != VALID_RESPONSE_CODE:
            return (DataFrame(), "", False)

        page_info: dict[str, str | bool] = response.json()["data"]["repository"][
            "issues"
        ]["pageInfo"]

        cursor: str = str(page_info["endCursor"])
        has_next_page: bool = bool(page_info["hasNextPage"])

        nodes: list[dict[str, dict[str, str]]] = response.json()["data"]["repository"][
            "issues"
        ]["edges"]

        return (DataFrame(data=map(itemgetter("node"), nodes)), cursor, has_next_page)
