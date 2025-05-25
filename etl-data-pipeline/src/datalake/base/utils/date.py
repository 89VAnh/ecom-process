from datetime import datetime, timezone


class DateUtils:

    @staticmethod
    def now():
        return datetime.now()

    @staticmethod
    def strftime(date, date_format=None):
        """convert datetime to string by date_format
        Args:
            date (datetime): datetime value
            date_format (string): format expect in result

        Returns:
            string:formated_date_value
        """
        return datetime.strftime(date, date_format)

    @staticmethod
    def strptime(date, date_format):
        """convert string to date using date_format
        Args:
            date (string): string date value
            date_format (string): format match with date value
        Returns:
            string: datetime value
        """
        return datetime.strptime(date, date_format)

    @staticmethod
    def min_date():
        return datetime.min

    @staticmethod
    def to_utc(date: datetime):
        return date.replace(tzinfo=timezone.utc)
