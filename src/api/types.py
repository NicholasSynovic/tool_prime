from pandas import DataFrame, Series
from pydantic import BaseModel, Field, ValidationError


def validate_df(model: BaseModel, df: DataFrame) -> None:
    def _run(data: Series) -> None:
        try:
            model(**data.to_dict())
        except ValidationError as ve:
            raise ve

    df.apply(f=_run, axis=1)


class CommitHashes(BaseModel):
    id: int = Field(default=..., description="Primary key")
    commit_hash: str = Field(default=..., description="Commit hash")
