import argparse
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.error import TransformError, RuntimeValueProviderError, PValueError, BeamError
from utils.constants import *


class SplitRow(beam.DoFn):
    def process(self, element):
        row_split = element.split(',')
        return [row_split]

class filter_transactions(beam.DoFn):
    def process(self, element):
        if float(element[3]) >= float(filter_transaction_amount):
            list_ = [element[0], element[3]]
            return [list_]

class date_check(beam.DoFn):
    def process(self, element):
        date =  datetime.datetime.strptime(element[0], format_date_input)
        if date.year > filter_transaction_date:
            date_filtered = [element[0], element[1]]
            return [date_filtered]

class date_change(beam.DoFn):
    def process(self, element):
        new_date_format = datetime.datetime.strptime(element[0],format_date_input).strftime(format_date_output)
        return [(new_date_format, float(element[1]))]

class result_format(beam.DoFn):
    def process(self, element):
        key, value = element
        return [f"{key}, {value}"]

class FiltersComposite(beam.PTransform):
    def expand(self, input_coll):
        try:
            return (
                input_coll
                | 'Split' >> beam.ParDo(SplitRow())
                | 'filter transactions' >> beam.ParDo(filter_transactions())
                | 'filter date' >> beam.ParDo(date_check())
                | 'change date' >> beam.ParDo(date_change())
                | 'Group and sum' >> beam.CombinePerKey(sum)
                | 'Result format' >> beam.ParDo(result_format())
                )

        except TransformError as transform_error:
            logging.error ( f"Transformation_filters: Transform Error: {transform_error}" )
            raise transform_error

        except PValueError as p_value:
            logging.error ( f"Transformation_filters: RunnerError: {p_value}" )
            raise p_value

        except  RuntimeValueProviderError as runtime_value_error:
            logging.error(f"Transformation _filters: RuntimeValueProviderError: {runtime_value_error}")
            raise runtime_value_error

        except  BeamError as beam_error:
            logging.error(f"Transformation _filters: BeamError: {beam_error}")
            raise beam_error