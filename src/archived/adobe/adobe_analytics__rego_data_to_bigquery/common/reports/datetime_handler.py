from datetime import datetime


def validate_date(date_text):
    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")
    return dt


def dts_start(date_text):
    # Converts a string formatted as '%Y-%m-%d' into a datetime object at 00:00:00
    date_text = validate_date(date_text)
    time_start = datetime.min.time()
    dts_start = datetime.strptime(
        datetime.combine(date_text, time_start).isoformat(),
        "%Y-%m-%dT%H:%M:%S",
    )

    return dts_start


def dts_end(date_text):
    # Converts a string formatted as '%Y-%m-%d' into a datetime object at 23:59:59
    date_text = validate_date(date_text)
    time_end = datetime.max.time()
    dts_end = datetime.strptime(
        datetime.combine(date_text, time_end).isoformat(),
        "%Y-%m-%dT%H:%M:%S.%f",
    )

    return dts_end
