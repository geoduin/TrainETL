from ..pipeline import Pipeline
from python import SQLHandler

class ConvertionPipeline(Pipeline):

    def __init__(self, sql_handler: SQLHandler):
        self.sql_handler = sql_handler
        super().__init__()

    def run(self):
        return super().run()
    