# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta

import pyarrow as pa

from kukur.source import Source, SourceWrapper, SeriesSelector, Metadata


class FakeSource():
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(self, _: SeriesSelector, start_date: datetime, end_date: datetime) -> pa.Table:
        # end_date should not be part of the returned interval, but is here for easy comparison
        return pa.Table.from_pydict({'ts': [start_date, end_date], 'value': [42, 24]})


class TestSourceWrapper():

    selector = SeriesSelector('fake', 'test-tag-1')
    start_date = datetime.fromisoformat('2020-01-01T00:00:00+00:00')
    end_date = datetime.fromisoformat('2020-02-01T00:00:00+00:00')

    def test_split_none(self):
        wrapper = SourceWrapper(_make_source(), [], {})
        result = wrapper.get_data(self.selector, self.start_date, self.end_date)
        assert len(result) == 2
        data = result.to_pydict()
        assert data['ts'][0] == self.start_date
        assert data['value'][0] == 42
        assert data['ts'][1] == self.end_date
        assert data['value'][1] == 24

    def test_split_equal_start_end(self):
        wrapper = SourceWrapper(_make_source(), [], {'data_query_interval_seconds': 24*60*60})
        result = wrapper.get_data(self.selector, self.start_date, self.start_date)
        assert len(result) == 0

    def test_split_one_day(self):
        wrapper = SourceWrapper(_make_source(), [], {'data_query_interval_seconds': 24*60*60})
        result = wrapper.get_data(self.selector, self.start_date, self.end_date)
        assert len(result) == 62
        data = result.to_pydict()
        assert data['ts'][0] == self.start_date
        assert data['value'][0] == 42
        assert data['ts'][1] == self.start_date + timedelta(seconds=24*60*60)
        assert data['value'][1] == 24
        assert data['ts'][61] == self.end_date
        assert data['value'][61] == 24

    def test_split_partial_end_interval(self):
        end_date = datetime.fromisoformat('2020-01-31T12:00:00+00:00')

        wrapper = SourceWrapper(_make_source(), [], {'data_query_interval_seconds': 24*60*60})
        result = wrapper.get_data(self.selector, self.start_date, end_date)
        assert len(result) == 62
        data = result.to_pydict()
        assert data['ts'][0] == self.start_date
        assert data['value'][0] == 42
        assert data['ts'][1] == self.start_date + timedelta(seconds=24*60*60)
        assert data['value'][1] == 24
        assert data['ts'][61] == end_date
        assert data['value'][61] == 24

    def test_split_partial_interval(self):
        end_date = self.start_date + timedelta(seconds=60)

        wrapper = SourceWrapper(_make_source(), [], {'data_query_interval_seconds': 24*60*60})
        result = wrapper.get_data(self.selector, self.start_date, end_date)
        assert len(result) == 2
        data = result.to_pydict()
        assert data['ts'][0] == self.start_date
        assert data['value'][0] == 42
        assert data['ts'][1] == end_date
        assert data['value'][1] == 24


def _make_source():
    source = FakeSource()
    return Source(source, source)
