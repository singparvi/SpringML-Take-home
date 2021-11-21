import argparse
import logging
# import re
# import pandas as pd

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# class WordExtractingDoFn(beam.DoFn):
#   """Parse each line of input text into words."""
#   def process(self, element):
#     """Returns an iterator over the words of this element.
#
#     The element is a line of text.  If the line is blank, note that, too.
#
#     Args:
#       element: the element being processed
#
#     Returns:
#       The processed element.
#     """
#     return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://gcp_bucket_springml/Data/AB_NYC_2019.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    # --Parvi edit
    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    # pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    #
    # # The pipeline will be run on exiting the with block.
    # with beam.Pipeline(options=pipeline_options) as p:
    #     # Read the text file[pattern] into a PCollection.
    #     lines = p | 'Read' >> ReadFromText(known_args.input)
    #
    #     transformandexport = (
    #             lines | 'GroupBy' >> (beam.GroupBy(lambda s: s[0]))
    #             | 'Write' >> WriteToText(known_args.output)
    #     )
    # --Parvi edit
    p = beam.Pipeline(options=PipelineOptions())


    # class GetTotal(beam.DoFn):
    #     def process(self, element):
    #         # get the total transactions for one item
    #         return [(str(element[0]), sum(element[1]))]

    data_from_source = (p
                        | 'ReadMyFile' >> ReadFromText(known_args.input)
                        | 'GroupBy the data' >> beam.GroupByKey()
                        | 'Export results to new file' >> WriteToText('Results', '.txt')
                        )
    result = p.run()

    # # Format the counts into a PCollection of strings.
    # def format_result(word, count):
    #   return '%s: %d' % (word, count)
    #
    # output = counts | 'Format' >> beam.MapTuple(format_result)
    #
    # # Write the output using a "Write" transform that has side effects.
    # # pylint: disable=expression-not-assigned
    # output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
