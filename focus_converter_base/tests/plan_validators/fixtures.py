from random import randint
from uuid import uuid4

import pytest

UML_GRAPH_HEADER = "graph LR;"


@pytest.fixture(scope="function")
def sample_provider_name():
    yield str(uuid4())


@pytest.fixture(scope="function")
def sample_column_name():
    yield str(uuid4())


@pytest.fixture(scope="function")
def sample_dimension_id():
    yield randint(0, 100)


@pytest.fixture(scope="function")
def sample_step_id():
    yield randint(0, 100)


@pytest.fixture(scope="function")
def sample_config_file_name(sample_dimension_id, sample_step_id):
    dimension_name = (
        "D"
        + "".join(["0"] * (3 - len(str(sample_dimension_id))))
        + str(sample_dimension_id)
    )

    step_name = (
        "S" + "".join(["0"] * (3 - len(str(sample_step_id)))) + str(sample_step_id)
    )

    yield f"{dimension_name}_{step_name}.yaml"
