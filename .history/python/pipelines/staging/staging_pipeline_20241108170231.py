from .pipeline import Pipeline
from python import Load, Extract, Transform

class StagingPipeline(Pipeline):
    sql_extracter: Extract
    sql_load: Load
    transformers: Transform

    def __init__(self):
        super().__init__()

    def run(self):
        return super().run()