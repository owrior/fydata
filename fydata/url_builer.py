import datetime as dt


def yahoo_url(ticker, start_date=dt.date(2010, 1, 1), end_date=dt.date.today(), period=None, frequency="1d"):
    """
    Function to create url string for data fetch from yahoo.
    :param ticker:
    :param start_date:
    :param end_date:
    :param period:
    :param frequency: 1d, 1wk, 1mo
    :return:
    """
    date_ref = dt.date(1970, 1, 1)

    if period is None:
        period1 = int((start_date - date_ref).total_seconds())
    else:
        start_date = end_date - period
        period1 = int((start_date - date_ref).total_seconds())

    period2 = int((end_date - date_ref).total_seconds())
    root = "https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={period1}&period2={period2}&interval={frequency}&events=history"

    built_url = root.format(ticker=ticker, period1=period1, period2=period2, frequency=frequency)

    return built_url
