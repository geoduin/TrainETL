from datetime import datetime

class DateKeyHandler:

    def convert_datetime_to_key(self, date:datetime) -> int:
        value = f"{str(date.year)}{str(date.month)}{str(date.day)}{str(date.hour)}{str(date.minute)}" 
        return int(value)