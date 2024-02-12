from typing import List, Literal

from pydantic import BaseModel


class StringTransformArgs(BaseModel):
    steps: List[
        Literal[
            "lower",
            "upper",
            "title",
        ]
    ]
