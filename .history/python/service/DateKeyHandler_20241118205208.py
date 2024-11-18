from datetime import datetime

class DateKeyHandler:

    def convert_datetime_to_key(self, date) -> int:
        print(date)
        print(type(date))
        year = self._append_zero(date.year) 
        month = self._append_zero(date.month) 
        day = self._append_zero(date.day)
        hour = self._append_zero(date.hour)
        minute = self._append_zero(date.minute)

        value = f"{year}{month}{day}{hour}{minute}" 
        return int(value)
    
    def _append_zero(self, val: int) -> str:
        if val < 10:
            return f"0{str(val)}"
        return str(val)