from ..pipeline import Pipeline

class ConvertionPipeline(Pipeline):

    def __init__(self):
        super().__init__()

    def run(self):
        return super().run()
    
    def create_dag(self):
        with self.dag:

            date_dimension_task = PythonOperator(
                task_id="create_date_tables",
                python_callable=self.create_datedimension_tables
            )

            python_task = PythonOperator(
                task_id="extract_raw_data",
                python_callable=self.pipeline.run
            )
            date_dimension_task >> python_task

        return self.dag