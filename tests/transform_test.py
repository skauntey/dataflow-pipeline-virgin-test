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
        "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
        "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
    ]
    output_data = [
        "2017-03-18, 2102.22"
    ]

    with TestPipeline() as tp:
        input = tp | beam.Create(input_data)
        output = input | FiltersComposite()

        assert_that(output, equal_to(output_data))

def float_case():

    input_data = [
        "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,000",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12",
    ]

    value_error = pytest.raises(ValueError)
    with TestPipeline() as tp:
        input = tp | beam.Create(input_data)
        output = input | FiltersComposite()

    assert(
        str(value_error.value) == "could not convert string to float: 'abc' [while running 'TransactionFilterReport/Split']"
    )