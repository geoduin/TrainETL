from pipeline import Pipeline
from python.etl_classes.etl_abstract import AbstractETL

class RawPipeline(Pipeline):
    etl_processes: list[AbstractETL]

    def __init__(self, etl_steps: list[AbstractETL]):
        self.etl_processes = etl_steps
    
    def run(self):
        # Runs through each etl process.
        for step in self.etl_processes:
            step.run()
