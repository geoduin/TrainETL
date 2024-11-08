from ..pipeline import Pipeline
from python import Load, Extract, Transform
import logging 

class CleaningPipeline(Pipeline):
    """
    This pipeline will ensure that all of the data will be cleaned
    """
    sql_extracter: Extract
    sql_load: Load
    transformers: Transform

    def run(self):
        logging.info("Hello world")
    