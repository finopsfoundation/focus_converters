from typing import List, Literal, Optional, Union

from pydantic import BaseModel


class StringSplitArgument(BaseModel):
    operation_type: Literal["split"]
    split_by: str
    index: Optional[int] = None


class StringTransformArgs(BaseModel):
    steps: List[
        Union[
            StringSplitArgument,
            Literal[
                "lower",
                "upper",
                "title",
            ],
        ]
    ]
