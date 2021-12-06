import pytest
from collections import namedtuple
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from filter_composite.transformation_filters import FiltersComposite
from utils.constants import *

Result = namedtuple('Result', ['date', 'value'])

@pytest.fixture
def setup():
    pass

def simple_case():
    input_data = [
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12"
    ]
    output_data = [
        "2018-02-27, 268.24"
    ]

    with TestPipeline() as tp:
        input = tp | beam.Create(input_data)
        output = input | FiltersComposite()

        assert_that(output, equal_to(output_data))

