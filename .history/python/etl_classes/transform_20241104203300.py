from etl_abstract import AbstractETL

class Transform(AbstractETL):
    next: AbstractETL

    def __init__(self):
        super().__init__()