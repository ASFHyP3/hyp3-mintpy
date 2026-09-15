from pathlib import Path

import pytest


@pytest.fixture
def test_data_directory():
    here = Path(__file__).parent
    return here / 'data'
