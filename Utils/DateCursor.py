from datetime import datetime, timedelta


class DateCursor:
    def __init__(self, start_date: str, end_date: str, step: dict, date_format: str = "%Y-%m-%dT%H:%M:%SZ"):
        self.date_format = date_format

        self.start_date = datetime.strptime(start_date, self.date_format)
        self.end_date = datetime.strptime(end_date, self.date_format)
        self.step = timedelta(**step)

        self.current_date = self.start_date
        self.finished = False

    def get(self):
        return None if self.finished else self.current_date.strftime(self.date_format)

    def get_formatted(self, date_format):
        return None if self.finished else self.current_date.strftime(date_format)

    def next(self):
        if self.finished: return None

        self.current_date += self.step
        if self.current_date > self.end_date:
            self.current_date = self.end_date
            self.finished = True

        return self.get()
