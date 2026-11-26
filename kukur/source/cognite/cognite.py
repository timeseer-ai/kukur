"""Kukur source for Cognite Data Fusion."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import itertools
import logging
import time
from collections.abc import Generator
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import Any

import pyarrow as pa

from kukur import (
    DataType,
    InterpolationType,
    Metadata,
    SeriesSearch,
    SeriesSelector,
    quality,
)
from kukur.auth import OIDCConfig
from kukur.exceptions import (
    DataNotFoundException,
    InvalidDataError,
    InvalidSourceException,
    KukurException,
    MissingModuleException,
)
from kukur.metadata import fields
from kukur.quality import Quality
from kukur.source.token_cache import Token, TokenCache

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger(__name__)

TIME_SERIES_VIEW = {
    "type": "view",
    "space": "cdf_cdm",
    "externalId": "CogniteTimeSeries",
    "version": "v1",
}
UNIT_VIEW = {
    "type": "view",
    "space": "cdf_cdm",
    "externalId": "CogniteUnit",
    "version": "v1",
}

_INSTANCE_PAGE_SIZE = 1000
_DATA_PAGE_SIZE = 100_000
_AGGREGATE_PAGE_SIZE = 10_000

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MIN_TIMESTAMP_MS = (datetime(1900, 1, 1, tzinfo=UTC) - _EPOCH) // timedelta(
    milliseconds=1
)
_MAX_TIMESTAMP_MS = (datetime(2100, 1, 1, tzinfo=UTC) - _EPOCH) // timedelta(
    milliseconds=1
)

_MAX_SECONDS_GRANULARITY = 120
_MAX_MINUTES_GRANULARITY_SECONDS = 120 * 60
_MAX_HOURS_GRANULARITY = 100_000

_STATUS_OPTIONS = {
    "includeStatus": True,
    "ignoreBadDataPoints": False,
    "treatUncertainAsBad": False,
}

_SEVERITY_SHIFT = 30
_SEVERITY_MASK = 0b11

_THROTTLED_STATUS_CODES = {
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.SERVICE_UNAVAILABLE,
}
_DEFAULT_THROTTLE_BACKOFF_COUNT = 5
_MAX_THROTTLE_SLEEP_SECONDS = 32

_FALLBACK_TOKEN_LIFETIME_SECONDS = 300

_REQUIRED_KEYS = ["base_url", "project", "oidc_token_url", "client_id", "client_secret"]


class CogniteError(KukurException):
    """Raised when Cognite Data Fusion or its identity provider return an error."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class CogniteConfiguration:
    """Connection properties of a Cognite Data Fusion project."""

    base_url: str
    project: str
    oidc: OIDCConfig
    spaces: list[str]
    timeout_seconds: float
    verify_ssl: bool
    proxy: str | None
    throttle_backoff_count: int

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "CogniteConfiguration":
        """Create from the source configuration."""
        for key in _REQUIRED_KEYS:
            if not data.get(key):
                raise InvalidSourceException(f'cognite sources require a "{key}" entry')
        spaces = data.get("spaces", [])
        if not isinstance(spaces, list) or not all(
            isinstance(space, str) for space in spaces
        ):
            raise InvalidSourceException('"spaces" must be a list of space names')
        base_url = data["base_url"].rstrip("/")
        return cls(
            base_url=base_url,
            project=data["project"],
            oidc=OIDCConfig(
                client_id=data["client_id"],
                client_secret=data["client_secret"],
                oidc_token_url=data["oidc_token_url"],
                scope=data.get("scope") or f"{base_url}/.default",
            ),
            spaces=spaces,
            timeout_seconds=data.get("timeout_seconds", 60),
            verify_ssl=data.get("verify_ssl", True),
            proxy=data.get("proxy"),
            throttle_backoff_count=data.get(
                "throttle_backoff_count", _DEFAULT_THROTTLE_BACKOFF_COUNT
            ),
        )


class _Sleeper:
    def sleep(self, sleep_seconds: float):
        time.sleep(sleep_seconds)


class _TokenCacheAuth:
    def __init__(self, config: CogniteConfiguration, token_cache: TokenCache):
        self.__config = config
        self.__token_cache = token_cache
        self.__replacement_token: Token | None = None

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self._get_access_token()}"
        return request

    def replace_token(self) -> None:
        self.__replacement_token = self._request_token(None)

    def _get_access_token(self) -> str:
        token = self.__replacement_token
        if token is not None and not token.is_expired():
            return token.access_token
        return self.__token_cache.get_token(self._request_token)

    def _request_token(self, _refresh_token: str | None) -> Token:
        oidc = self.__config.oidc
        with _new_session(self.__config) as session:
            response = session.post(
                oidc.oidc_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": oidc.client_id,
                    "client_secret": oidc.client_secret,
                    "scope": oidc.scope,
                },
                timeout=self.__config.timeout_seconds,
            )
            if response.status_code >= HTTPStatus.BAD_REQUEST:
                raise CogniteError(
                    f"Token request failed with status {response.status_code}: {_error_message(response)}",
                    response.status_code,
                )
            token_data = response.json()
        expires_in = int(token_data.get("expires_in", _FALLBACK_TOKEN_LIFETIME_SECONDS))
        return Token(
            token_data["access_token"],
            datetime.now(tz=UTC) + timedelta(seconds=expires_in),
        )


class CogniteSource:
    """Read the time series of a Cognite Data Fusion project."""

    def __init__(self, config: CogniteConfiguration, token_cache: TokenCache):
        if not HAS_REQUESTS:
            raise MissingModuleException("requests", "cognite")
        self.__config = config
        self.__auth = _TokenCacheAuth(config, token_cache)
        self._sleeper = _Sleeper()

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all time series in the project, or in the configured spaces."""
        unit_symbols: dict[tuple[str, str], str] = {}
        requested_units: set[tuple[str, str]] = set()
        can_read_units = True
        state_count = 0
        body: dict[str, Any] = {
            "instanceType": "node",
            "sources": [{"source": TIME_SERIES_VIEW}],
            "filter": self._get_search_filter(),
            "limit": _INSTANCE_PAGE_SIZE,
        }
        with self._session() as session:
            while True:
                response = self._post(session, "models/instances/list", body)
                items = response["items"]

                if can_read_units:
                    unit_ids = {
                        unit_id
                        for item in items
                        if (
                            unit_id := _get_unit_id(
                                _get_view_properties(item, TIME_SERIES_VIEW)
                            )
                        )
                        is not None
                    } - requested_units
                    if len(unit_ids) > 0:
                        symbols = self._get_unit_symbols(session, unit_ids)
                        if symbols is None:
                            can_read_units = False
                        else:
                            unit_symbols.update(symbols)
                            requested_units.update(unit_ids)

                for item in items:
                    properties = _get_view_properties(item, TIME_SERIES_VIEW)
                    if properties.get("type") == "state":
                        state_count = state_count + 1
                        continue
                    series = SeriesSelector(
                        selector.source,
                        self._get_tags(item["space"], item["externalId"]),
                    )
                    yield _to_metadata(series, properties, unit_symbols)

                cursor = response.get("nextCursor")
                if cursor is None:
                    break
                body = body | {"cursor": cursor}
        if state_count > 0:
            logger.info(
                "Skipped %s state time series in Cognite project %s",
                state_count,
                self.__config.project,
            )

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return the metadata of one time series."""
        instance_id = self._get_instance_id(selector)
        with self._session() as session:
            response = self._post(
                session,
                "models/instances/byids",
                {
                    "items": [{"instanceType": "node", **instance_id}],
                    "sources": [{"source": TIME_SERIES_VIEW}],
                },
            )
            properties: dict[str, Any] = {}
            if len(response["items"]) > 0:
                properties = _get_view_properties(
                    response["items"][0], TIME_SERIES_VIEW
                )
            if len(properties) == 0:
                raise self._not_found(selector)
            _check_supported(selector, properties.get("type"))
            unit_symbols: dict[tuple[str, str], str] = {}
            unit_id = _get_unit_id(properties)
            if unit_id is not None:
                unit_symbols = self._get_unit_symbols(session, {unit_id}) or {}
        return _to_metadata(selector, properties, unit_symbols)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return the data points of one time series, including bad and uncertain ones."""
        start_ms, end_ms = _to_time_range(start_date, end_date)
        if start_ms >= end_ms:
            return _combine_pages([], is_string=False)
        query = {
            "instanceId": self._get_instance_id(selector),
            "start": start_ms,
            "end": end_ms,
            "limit": _DATA_PAGE_SIZE,
            **_STATUS_OPTIONS,
        }
        tables = []
        is_string: bool | None = None
        with closing(self._list_pages(selector, query)) as pages:
            for page in pages:
                if is_string is None:
                    is_string = _is_string(selector, page)
                tables.append(
                    _to_table(page.get("datapoints", []), is_string=is_string)
                )
        return _combine_pages(tables, is_string=bool(is_string))

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return the minimum and maximum data point of each interval, at the time they occurred."""
        start_ms, end_ms = _to_time_range(start_date, end_date)
        granularity = _get_granularity(start_ms, end_ms, interval_count)
        if granularity is None:
            return self.get_data(selector, start_date, end_date)
        instance_id = self._get_instance_id(selector)
        query = {
            "instanceId": instance_id,
            "start": start_ms,
            "end": end_ms,
            "aggregates": ["minDatapoint", "maxDatapoint"],
            "granularity": granularity,
            "limit": _AGGREGATE_PAGE_SIZE,
            **_STATUS_OPTIONS,
        }
        tables = []
        with closing(self._list_pages(selector, query)) as pages:
            try:
                first_page = next(pages)
            except CogniteError as err:
                if err.status_code != HTTPStatus.BAD_REQUEST or not (
                    self._is_string_series(selector, instance_id)
                ):
                    raise
                logger.debug(
                    'String time series "%s" has no aggregates, returning all data points',
                    selector.name,
                )
                return self.get_data(selector, start_date, end_date)
            _check_supported(selector, first_page.get("type"))
            for page in itertools.chain([first_page], pages):
                tables.append(
                    _to_table(
                        _select_extremes(page.get("datapoints", []), start_ms, end_ms),
                        is_string=False,
                    )
                )
        return _combine_pages(tables, is_string=False)

    def _list_pages(
        self, selector: SeriesSelector, query: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        with self._session() as session:
            while True:
                response = self._post(
                    session,
                    "timeseries/data/list",
                    {"items": [query], "ignoreUnknownIds": True},
                )
                if len(response["items"]) == 0:
                    raise self._not_found(selector)
                series = response["items"][0]
                cursor = series.get("nextCursor")
                yield series
                if cursor is None:
                    return
                query = query | {"cursor": cursor}

    def _is_string_series(
        self, selector: SeriesSelector, instance_id: dict[str, str]
    ) -> bool:
        with self._session() as session:
            response = self._post(
                session,
                "timeseries/data/list",
                {
                    "items": [{"instanceId": instance_id, "limit": 1}],
                    "ignoreUnknownIds": True,
                },
            )
        if len(response["items"]) == 0:
            raise self._not_found(selector)
        return _is_string(selector, response["items"][0])

    def _not_found(self, selector: SeriesSelector) -> DataNotFoundException:
        return DataNotFoundException(
            f'Time series "{selector.name}" not found in Cognite project "{self.__config.project}"'
        )

    def _get_unit_symbols(
        self, session, unit_ids: set[tuple[str, str]]
    ) -> dict[tuple[str, str], str] | None:
        try:
            response = self._post(
                session,
                "models/instances/byids",
                {
                    "items": [
                        {
                            "instanceType": "node",
                            "space": space,
                            "externalId": external_id,
                        }
                        for space, external_id in sorted(unit_ids)
                    ],
                    "sources": [{"source": UNIT_VIEW}],
                },
            )
        except CogniteError as err:
            if err.status_code != HTTPStatus.FORBIDDEN:
                raise
            logger.warning(
                "Cannot read units in Cognite project %s, using source units: %s",
                self.__config.project,
                err,
            )
            return None
        symbols = {}
        for item in response["items"]:
            symbol = _get_view_properties(item, UNIT_VIEW).get("symbol")
            if symbol:
                symbols[(item["space"], item["externalId"])] = symbol
        return symbols

    def _get_search_filter(self) -> dict[str, Any]:
        has_data = {"hasData": [TIME_SERIES_VIEW]}
        if len(self.__config.spaces) == 0:
            return has_data
        return {
            "and": [
                has_data,
                {
                    "in": {
                        "property": ["node", "space"],
                        "values": self.__config.spaces,
                    }
                },
            ]
        }

    def _get_tags(self, space: str, external_id: str) -> dict[str, str]:
        if len(self.__config.spaces) == 1:
            return {"series name": external_id}
        return {"series name": external_id, "space": space}

    def _get_instance_id(self, selector: SeriesSelector) -> dict[str, str]:
        external_id = selector.tags.get("series name")
        if external_id is None:
            raise DataNotFoundException(
                f'Series "{selector.name}" has no "series name" tag'
            )
        spaces = self.__config.spaces
        space = selector.tags.get("space")
        if space is None:
            if len(spaces) != 1:
                raise DataNotFoundException(
                    f'Series "{selector.name}" has no "space" tag'
                )
            space = spaces[0]
        elif len(spaces) > 0 and space not in spaces:
            raise DataNotFoundException(
                f'Series "{selector.name}" is in space "{space}", which is not one of the configured spaces'
            )
        return {"space": space, "externalId": external_id}

    def _session(self) -> "requests.Session":
        session = _new_session(self.__config)
        session.auth = self.__auth
        return session

    def _post(self, session, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.__config.base_url}/api/v1/projects/{self.__config.project}/{path}"
        throttle_count = 1
        token_replaced = False
        while True:
            response = session.post(
                url, json=body, timeout=self.__config.timeout_seconds
            )
            if response.status_code == HTTPStatus.UNAUTHORIZED and not token_replaced:
                logger.info("Cognite rejected the access token, requesting a new one")
                self.__auth.replace_token()
                token_replaced = True
                continue
            if (
                response.status_code in _THROTTLED_STATUS_CODES
                and throttle_count <= self.__config.throttle_backoff_count
            ):
                sleep_seconds = min(2**throttle_count, _MAX_THROTTLE_SLEEP_SECONDS)
                logger.info(
                    "Cognite request to %s throttled (%s), sleeping for %s seconds",
                    path,
                    response.status_code,
                    sleep_seconds,
                )
                self._sleeper.sleep(sleep_seconds)
                throttle_count = throttle_count + 1
                continue
            if response.status_code >= HTTPStatus.BAD_REQUEST:
                raise CogniteError(
                    f"Cognite request to {path} failed with status {response.status_code}: {_error_message(response)}",
                    response.status_code,
                )
            return response.json()


def from_config(config: dict[str, Any], token_cache: TokenCache) -> CogniteSource:
    """Create a new Cognite Data Fusion source."""
    cognite_config = CogniteConfiguration.from_data(config)
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "cognite")
    return CogniteSource(cognite_config, token_cache)


def _new_session(config: CogniteConfiguration) -> "requests.Session":
    session = requests.Session()
    if config.proxy:
        session.proxies.update({"https": config.proxy})
    if not config.verify_ssl:
        session.verify = False
    return session


def _error_message(response) -> str:
    try:
        data = response.json()
    except ValueError:
        return response.text
    if not isinstance(data, dict):
        return response.text
    error = data.get("error")
    if isinstance(error, dict):
        return str(error.get("message", error))
    if "error_description" in data:
        return str(data["error_description"])
    return response.text


def _get_view_properties(item: dict[str, Any], view: dict[str, str]) -> dict[str, Any]:
    return (
        item.get("properties", {})
        .get(view["space"], {})
        .get(f"{view['externalId']}/{view['version']}", {})
    )


def _get_unit_id(properties: dict[str, Any]) -> tuple[str, str] | None:
    unit = properties.get("unit")
    if not isinstance(unit, dict) or "space" not in unit or "externalId" not in unit:
        return None
    return (unit["space"], unit["externalId"])


def _to_metadata(
    selector: SeriesSelector,
    properties: dict[str, Any],
    unit_symbols: dict[tuple[str, str], str],
) -> Metadata:
    metadata = Metadata(selector)
    if description := properties.get("description"):
        metadata.set_field(fields.Description, description)

    unit = None
    unit_id = _get_unit_id(properties)
    if unit_id is not None:
        unit = unit_symbols.get(unit_id)
    if unit is None:
        unit = properties.get("sourceUnit")
    if unit:
        metadata.set_field(fields.Unit, unit)

    series_type = properties.get("type")
    if series_type == "numeric":
        metadata.set_field(fields.DataType, DataType.FLOAT64)
    elif series_type == "string":
        metadata.set_field(fields.DataType, DataType.STRING)

    if properties.get("isStep", False):
        metadata.set_field(fields.InterpolationType, InterpolationType.STEPPED)
    else:
        metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)
    return metadata


def _check_supported(selector: SeriesSelector, series_type: str | None) -> None:
    if series_type == "state":
        raise InvalidDataError(
            f'"{selector.name}" is a state time series, which is not supported'
        )


def _is_string(selector: SeriesSelector, series: dict[str, Any]) -> bool:
    series_type = series.get("type")
    _check_supported(selector, series_type)
    return bool(series.get("isString", series_type == "string"))


def _to_value(value: Any, is_string: bool) -> Any:
    if value is None:
        return None
    if is_string:
        return str(value)
    return float(value)


def _to_quality(status: dict[str, Any] | None) -> int:
    if status is None:
        return Quality.GOOD.value
    code = status.get("code")
    if code is None:
        if str(status.get("symbol", "")).startswith("Good"):
            return Quality.GOOD.value
        return Quality.BAD.value
    if (int(code) >> _SEVERITY_SHIFT) & _SEVERITY_MASK == 0:
        return Quality.GOOD.value
    return Quality.BAD.value


def _select_extremes(
    aggregates: list[dict[str, Any]], start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    data_points: list[dict[str, Any]] = []
    for aggregate in aggregates:
        interval_points = sorted(
            (
                aggregate[name]
                for name in ["minDatapoint", "maxDatapoint"]
                if name in aggregate
            ),
            key=lambda data_point: data_point["timestamp"],
        )
        for data_point in interval_points:
            if not start_ms <= data_point["timestamp"] < end_ms:
                continue
            if (
                len(data_points) > 0
                and data_points[-1]["timestamp"] == data_point["timestamp"]
            ):
                continue
            data_points.append(data_point)
    return data_points


def _to_table(data_points: list[dict[str, Any]], *, is_string: bool) -> pa.Table:
    return pa.Table.from_pydict(
        {
            "ts": pa.array(
                [data_point["timestamp"] for data_point in data_points],
                pa.timestamp("ms", "UTC"),
            ).cast(pa.timestamp("us", "UTC")),
            "value": pa.array(
                [
                    _to_value(data_point.get("value"), is_string)
                    for data_point in data_points
                ],
                pa.string() if is_string else pa.float64(),
            ),
            "quality": pa.array(
                [_to_quality(data_point.get("status")) for data_point in data_points],
                pa.int16(),
            ),
        }
    )


def _combine_pages(tables: list[pa.Table], *, is_string: bool) -> pa.Table:
    if len(tables) == 0:
        tables = [_to_table([], is_string=is_string)]
    return quality.set_mapping(pa.concat_tables(tables), quality.DEFAULT_MAPPING)


def _to_time_range(start_date: datetime, end_date: datetime) -> tuple[int, int]:
    return (
        max(_to_epoch_ms(start_date), _MIN_TIMESTAMP_MS),
        min(_to_epoch_ms(end_date), _MAX_TIMESTAMP_MS),
    )


def _to_epoch_ms(timestamp: datetime) -> int:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return (timestamp - _EPOCH) // timedelta(milliseconds=1)


def _get_granularity(start_ms: int, end_ms: int, interval_count: int) -> str | None:
    seconds = (end_ms - start_ms) // 1000 // max(interval_count, 1)
    if seconds < 1:
        return None
    if seconds < _MAX_SECONDS_GRANULARITY:
        return f"{seconds}s"
    if seconds < _MAX_MINUTES_GRANULARITY_SECONDS:
        return f"{seconds // 60}m"
    return f"{min(seconds // 3600, _MAX_HOURS_GRANULARITY)}h"
