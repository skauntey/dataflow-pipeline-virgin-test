# importing modules
# Change the data output location later
import argparse
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.error import *
from filter_composite.transformation_filters import *
from utils.constants import *


# Transaction pipeline : connection and executing pipeline
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser ()
    parser.add_argument("--input",
                        dest='input',
                        default="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
                        help='The file path for the input text to process.')

    parser.add_argument("--output",
                        dest='output',
                        default="gs://data_files01/output/result",
                        help='Output file to write results to.' )

    path_args, beam_args = parser.parse_known_args()

    #DataFlow Pipeline options
    beam_args.extend (
            [
                "--project=beam-334214",
                "--runner=DirectRunner",
                "--temp_location=gs://data_temp01/tmp",
                "--staging_location=gs://data_temp01/tmp",
                "--region=europe-west2",
                "--job_name=pipeline-filtering"
            ]
    )
    return path_args , beam_args

# Executing pipeline

def execute_pipe():
    path_args, beam_args = run(save_main_session=True)

    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True

    inputs_pattern = path_args.input
    outputs_prefix = path_args.output

    with beam.Pipeline(options=options) as pipeline:
        csv_data = (
                pipeline
                | 'Read lines' >> beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
                | 'Transformation Pipeline' >> FiltersComposite()
                | 'Write results' >> beam.io.WriteToText(outputs_prefix)
                    )
        try:
            return csv_data

        except PipelineError as pipe_error:
            logging.error ( f"Pipeline Error Message: {pipe_error}" )
            raise pipe_error

        except RunnerError as runner_error:
            logging.error ( f"RunnerError Message: {runner_error}" )
            raise runner_error

        except Exception as e:
            logging.error(f"Generic exception!")



"""file_name_suffix = zip_format,append_trailing_newlines = True,compression_type = CompressionTypes.GZIP,header=header_transaction_output"""
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    execute_pipe()