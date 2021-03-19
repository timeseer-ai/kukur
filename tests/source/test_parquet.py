"""Test the Parquet time series source."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from dateutil.parser import parse as parse_date

from kukur import SeriesSelector
from kukur.source.parquet import from_config


class TestFeather():

    start_date = parse_date('2020-01-01T00:00:00Z')
    end_date = parse_date('2020-11-01T00:00:00Z')
    series_id = SeriesSelector('fake', 'test-tag-1')

    def test_dir(self):
        source = from_config({'path': 'tests/test_data/parquet/dir', 'format': 'dir'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 5
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0

    def test_row(self):
        source = from_config({'path': 'tests/test_data/parquet/row.parquet', 'format': 'row'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 5
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0

    def test_pivot(self):
        source = from_config({'path': 'tests/test_data/parquet/pivot.parquet', 'format': 'pivot'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 7
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0
