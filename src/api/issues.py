from requests import Response, post
from pandas import DataFrame
import pandas as pd
from string import Template


def query_graphql(
    url: str, json_query: str, headers: dict[str, str], timeout: int = 60
) -> Response:
    return post(
        url=url,
        json={"query": json_query},
        headers=headers,
        timeout=timeout,
    )


class GitHubIssues:
    def __init__(self, owner: str, repo_name: str, auth_key: str) -> None:
        self.base_url: str = "https://api.github.com/graphql"
        self.owner: str = owner
        self.repo_name: str = repo_name
        self.auth_key: str = auth_key

    def get_total_issues(self) -> int:
        json_template: Template = Template(
            template='query { repository(name:"$name", owner:"$owner"){issues {totalCount}}, rateLimit {limit, cost, remaining, resetAt}}'  # noqa: E501
        )
        json_query = json_template.substitute(name=self.repo_name, owner=self.owner)

        headers: dict[str, str] = {
            "Authorization": f"Bearer {self.auth_key}",
            "Content-Type": "application/json",
        }

        response: Response = query_graphql(
            url=self.base_url, json_query=json_query, headers=headers
        )

        if response.status_code != 200:
            return -1

        return response.json()["data"]["repository"]["issues"]["totalCount"]

    def get_issues(self) -> DataFrame:
        data: list[DataFrame] = []
        return pd.concat(objs=data, ignore_index=True)


ghi: GitHubIssues = GitHubIssues(owner="numpy", repo_name="numpy", auth_key="test")
ghi.get_total_issues()
