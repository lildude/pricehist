import importlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pricehist import beanprice, exceptions, sources
from pricehist.price import Price
from pricehist.series import Series


@pytest.fixture
def series():
    series = Series(
        "BTC",
        "USD",
        "high",
        "2021-01-01",
        "2021-01-03",
        prices=[
            Price("2021-01-01", Decimal("1.1")),
            Price("2021-01-02", Decimal("1.2")),
            Price("2021-01-03", Decimal("1.3")),
        ],
    )
    return series


@pytest.fixture
def pricehist_source(mocker, series):
    mock = mocker.MagicMock()
    mock.types = mocker.MagicMock(return_value=["close", "high", "low"])
    mock.fetch = mocker.MagicMock(return_value=series)
    return mock


@pytest.fixture
def source(pricehist_source):
    return beanprice.source(pricehist_source)()


@pytest.fixture
def ltz():
    return datetime.now(timezone.utc).astimezone().tzinfo


def test_get_prices_series(pricehist_source, source, ltz):
    ticker = "BTC:USD:high"
    begin = datetime(2021, 1, 1, tzinfo=ltz)
    end = datetime(2021, 1, 3, tzinfo=ltz)
    result = source.get_prices_series(ticker, begin, end)

    pricehist_source.fetch.assert_called_once_with(
        Series("BTC", "USD", "high", "2021-01-01", "2021-01-03")
    )

    assert result == [
        beanprice.SourcePrice(Decimal("1.1"), datetime(2021, 1, 1, tzinfo=ltz), "USD"),
        beanprice.SourcePrice(Decimal("1.2"), datetime(2021, 1, 2, tzinfo=ltz), "USD"),
        beanprice.SourcePrice(Decimal("1.3"), datetime(2021, 1, 3, tzinfo=ltz), "USD"),
    ]


def test_get_prices_series_exception(pricehist_source, source, ltz, mocker):
    pricehist_source.fetch = mocker.MagicMock(
        side_effect=exceptions.RequestError("Message")
    )
    ticker = "_5eDJI::low"
    begin = datetime(2021, 1, 1, tzinfo=ltz)
    end = datetime(2021, 1, 3, tzinfo=ltz)
    result = source.get_prices_series(ticker, begin, end)
    assert result is None


def test_get_prices_series_special_chars(pricehist_source, source, ltz):
    ticker = "_5eDJI::low"
    begin = datetime(2021, 1, 1, tzinfo=ltz)
    end = datetime(2021, 1, 3, tzinfo=ltz)
    source.get_prices_series(ticker, begin, end)
    pricehist_source.fetch.assert_called_once_with(
        Series("^DJI", "", "low", "2021-01-01", "2021-01-03")
    )


def test_get_prices_series_price_type(pricehist_source, source, ltz):
    ticker = "TSLA"
    begin = datetime(2021, 1, 1, tzinfo=ltz)
    end = datetime(2021, 1, 3, tzinfo=ltz)
    source.get_prices_series(ticker, begin, end)
    pricehist_source.fetch.assert_called_once_with(
        Series("TSLA", "", "close", "2021-01-01", "2021-01-03")
    )


def test_get_prices_series_gbx_gbp_converts_to_gbp(
    pricehist_source, source, ltz, mocker
):
    pricehist_source.fetch = mocker.MagicMock(
        return_value=Series(
            "FWRG.L",
            "GBX",
            "close",
            "2021-01-01",
            "2021-01-03",
            prices=[
                Price("2021-01-01", Decimal("123.4")),
                Price("2021-01-02", Decimal("250")),
            ],
        )
    )
    ticker = "FWRG.L:GBX:close"
    begin = datetime(2021, 1, 1, tzinfo=ltz)
    end = datetime(2021, 1, 2, tzinfo=ltz)

    result = source.get_prices_series(ticker, begin, end)

    assert result == [
        beanprice.SourcePrice(
            Decimal("1.234"), datetime(2021, 1, 1, tzinfo=ltz), "GBP"
        ),
        beanprice.SourcePrice(Decimal("2.5"), datetime(2021, 1, 2, tzinfo=ltz), "GBP"),
    ]


    def test_get_prices_series_yahoo_with_explicit_gbx_quote_retries_without_quote(
        ltz, mocker
    ):
        pricehist_source = mocker.MagicMock()
        pricehist_source.id = mocker.MagicMock(return_value="yahoo")
        pricehist_source.types = mocker.MagicMock(return_value=["close", "high", "low"])

        invalid = exceptions.InvalidPair(
            "FWRG.L", "GBX", pricehist_source, "Don't specify the quote currency."
        )
        converted_series = Series(
            "FWRG.L",
            "GBX",
            "close",
            "2021-01-01",
            "2021-01-02",
            prices=[
                Price("2021-01-01", Decimal("123.4")),
                Price("2021-01-02", Decimal("250")),
            ],
        )
        pricehist_source.fetch = mocker.MagicMock(side_effect=[invalid, converted_series])
        subject = beanprice.source(pricehist_source)()

        result = subject.get_prices_series(
            "FWRG.L:GBX:close",
            datetime(2021, 1, 1, tzinfo=ltz),
            datetime(2021, 1, 2, tzinfo=ltz),
        )

        assert result == [
            beanprice.SourcePrice(Decimal("1.234"), datetime(2021, 1, 1, tzinfo=ltz), "GBP"),
            beanprice.SourcePrice(Decimal("2.5"), datetime(2021, 1, 2, tzinfo=ltz), "GBP"),
        ]
        assert pricehist_source.fetch.call_count == 2
        assert pricehist_source.fetch.call_args_list[0].args[0] == Series(
            "FWRG.L", "GBX", "close", "2021-01-01", "2021-01-02"
        )
        assert pricehist_source.fetch.call_args_list[1].args[0] == Series(
            "FWRG.L", "", "close", "2021-01-01", "2021-01-02"
        )


def test_get_historical_price(pricehist_source, source, ltz):
    ticker = "BTC:USD:high"
    time = datetime(2021, 1, 3, tzinfo=ltz)
    result = source.get_historical_price(ticker, time)
    pricehist_source.fetch.assert_called_once_with(
        Series("BTC", "USD", "high", "2021-01-03", "2021-01-03")
    )
    assert result == beanprice.SourcePrice(
        Decimal("1.3"), datetime(2021, 1, 3, tzinfo=ltz), "USD"
    )


def test_get_historical_price_none_available(pricehist_source, source, ltz, mocker):
    pricehist_source.fetch = mocker.MagicMock(
        return_value=Series("BTC", "USD", "high", "2021-01-03", "2021-01-03", prices=[])
    )
    ticker = "BTC:USD:high"
    time = datetime(2021, 1, 3, tzinfo=ltz)
    result = source.get_historical_price(ticker, time)
    assert result is None


def test_get_latest_price(pricehist_source, source, ltz):
    ticker = "BTC:USD:high"
    start = datetime.combine((date.today() - timedelta(days=7)), datetime.min.time())
    today = datetime.combine(date.today(), datetime.min.time())
    result = source.get_latest_price(ticker)
    pricehist_source.fetch.assert_called_once_with(
        Series("BTC", "USD", "high", start.date().isoformat(), today.date().isoformat())
    )
    assert result == beanprice.SourcePrice(
        Decimal("1.3"), datetime(2021, 1, 3, tzinfo=ltz), "USD"
    )


def test_get_latest_price_none_available(pricehist_source, source, ltz, mocker):
    pricehist_source.fetch = mocker.MagicMock(
        return_value=Series("BTC", "USD", "high", "2021-01-01", "2021-01-03", prices=[])
    )
    ticker = "BTC:USD:high"
    result = source.get_latest_price(ticker)
    assert result is None


def test_all_sources_available_for_beanprice():
    for identifier in sources.by_id.keys():
        importlib.import_module(f"pricehist.beanprice.{identifier}").Source()
