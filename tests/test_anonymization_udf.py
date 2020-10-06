import pytest
from analyticsbackend.anonymization_pipeline import AnonymizationUDF


@pytest.fixture(scope="module")
def anonymization_udf():
    return AnonymizationUDF()


testdata = [
    ("hello", "olleh"),
    ("john", "nhoj"),
    ("databricks", "skcirbatad"),
    ("", ""),
    (None, None),
]


@pytest.mark.parametrize("input_name, expected", testdata)
def test_build_optimize_query(anonymization_udf, input_name, expected):
    assert anonymization_udf.anonymize_name(input_name) == expected
