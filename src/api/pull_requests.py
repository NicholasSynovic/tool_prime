"""
Issue tracker interfaces and classes.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pandas import DataFrame
from string import Template
from functools import partial
from operator import itemgetter
from src.api import VALID_RESPONSE_CODE
import pandas as pd
from src.api.utils import query_graphql
from requests import Response


class GitHubPullRequests:
    """
    Class to interact with the GitHub GraphQL API to fetch pull request metadata.

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
        get_total_pull requests() -> int:
            Retrieves the total number of pull requests in the repository.

        get_pull requests(after_cursor: str = "null") -> tuple[pd.DataFrame, str, bool]:
            Retrieves paginated pull request metadata from the repository.

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

    def get_total_pull_requests(self) -> int:
        """
        Query the GitHub GraphQL API to retrieve the total number of pull requests.

        Returns:
            int: The total count of pull requests if the request is successful;
                -1 if the request fails (e.g., due to a non-200 status code).

        """
        json_template: Template = Template(
            template="""
            query {
                repository(name:"$name", owner:"$owner") {
                    pull_requests {
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

        return response.json()["data"]["repository"]["pull requests"]["totalCount"]

    def get_pull_requests(
        self, after_cursor: str = "null"
    ) -> tuple[DataFrame, str, bool]:
        """
        Retrieve a page of GitHub pull requests from a repository using GraphQL.

        Constructs and sends a GraphQL query to retrieve up to 100 pull requests from
        the specified repository, starting after the provided cursor. Returns
        the pull requests in a DataFrame, along with the cursor for the next page and a
        boolean indicating if more pages exist.

        Args:
            after_cursor (str, optional): The pagination cursor to continue fetching
                pull requests from. Use "null" to start from the beginning.
                Defaults to "null".

        Returns:
            tuple[DataFrame, str, bool]:
                - A DataFrame containing pull request data
                    (`id`, `createdAt`, `closedAt`).
                - A string representing the end cursor for pagination.
                - A boolean indicating whether there are more pages of pull requests
                    to fetch.

        """
        json_template: Template = Template(
            template="""query {
                repository(name:"$name", owner:"$owner") {
                    pull_requests(first: 100, after: $cursor) {
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
            "pull requests"
        ]["pageInfo"]

        cursor: str = str(page_info["endCursor"])
        has_next_page: bool = bool(page_info["hasNextPage"])

        nodes: list[dict[str, dict[str, str]]] = response.json()["data"]["repository"][
            "pull requests"
        ]["edges"]

        pull_request_data: DataFrame = DataFrame(data=map(itemgetter("node"), nodes))

        pull_request_data = pull_request_data.rename(
            columns={
                "id": "pull request_id",
                "createdAt": "created_at",
                "closedAt": "closed_at",
            }
        )

        pull_request_data["created_at"] = pd.to_datetime(
            pull_request_data["created_at"],
            utc=True,
        )
        pull_request_data["closed_at"] = pd.to_datetime(
            pull_request_data["closed_at"],
            utc=True,
        )

        return (pull_request_data, cursor, has_next_page)
