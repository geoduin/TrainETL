from datetime import datetime

class DateKeyHandler:

    def convert_datetime_to_key(self, date:datetime) -> int:
        value = str(date.date)
        return NotImplementedError("No implementation")