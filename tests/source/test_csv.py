"""Test the Parquet time series source."""

from dateutil.parser import parse as parse_date

from kukur import SeriesSelector, DataType, Dictionary
from kukur.source.csv import from_config


class TestCsv():

    start_date = parse_date('2020-01-01T00:00:00Z')
    end_date = parse_date('2020-11-01T00:00:00Z')
    series_id = SeriesSelector('fake', 'test-tag-1')

    def test_dir(self):
        source = from_config({'path': 'tests/test_data/csv/dir', 'format': 'dir'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 5
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0

    def test_row(self):
        source = from_config({'path': 'tests/test_data/csv/row.csv', 'format': 'row'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 5
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0

    def test_pivot(self):
        source = from_config({'path': 'tests/test_data/csv/pivot.csv', 'format': 'pivot'})
        table = source.get_data(self.series_id, self.start_date, self.end_date)
        assert len(table) == 7
        assert table.column_names == ['ts', 'value']
        assert table['ts'][0].as_py() == self.start_date
        assert table['value'][0].as_py() == 1.0

    def test_row_metadata(self):
        source = from_config({'metadata': 'tests/test_data/csv/row-metadata.csv'})
        metadata = source.get_metadata(self.series_id)
        assert metadata.series == self.series_id
        assert isinstance(metadata.description, str)
        assert isinstance(metadata.unit, str)
        assert isinstance(metadata.limit_low, float)
        assert isinstance(metadata.limit_high, float)
        assert isinstance(metadata.accuracy, float)

    def test_row_metadata_dictionary(self):
        source = from_config({
            'metadata': 'tests/test_data/csv/row-metadata.csv',
            'dictionary_dir': 'tests/test_data/csv/dictionary'
        })
        metadata = source.get_metadata(SeriesSelector('fake', 'test-tag-6'))
        assert metadata.series == SeriesSelector('fake', 'test-tag-6')
        assert metadata.data_type == DataType.DICTIONARY
        assert metadata.dictionary_name == 'Active'
        assert isinstance(metadata.dictionary, Dictionary)
