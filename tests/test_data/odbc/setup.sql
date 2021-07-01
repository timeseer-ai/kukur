create database TestData;
go
use TestData;
go
create table Metadata (
    name nvarchar(max),
    description nvarchar(max),
    units nvarchar(max),
    data_type nvarchar(max),
    dictionary_name nvarchar(max),
    stepped_interpolation int
);
go
create table Dictionary (
    name nvarchar(max),
    value int,
    label nvarchar(max)
);
go
create table Data (
    name nvarchar(max),
    ts datetime2,
    str_ts nvarchar(max),
    value float(53),
    str_value nvarchar(max)
);
go
if 0 = (select count(*) from Metadata)
    insert into Metadata (name, description, units, data_type, dictionary_name, stepped_interpolation)
    values
    ('test-tag-1', 'A test tag', 'm', 'float64', NULL, 0),
    ('test-tag-4', 'A stepped test tag', 'm', 'float64', NULL, 1),
    ('test-tag-5', 'A string series', '', 'string', NULL, 1),
    ('test-tag-6', 'A dictionary series', '', 'dictionary', 'Active', 1);
go
if 0 = (select count(*) from Dictionary)
    insert into Dictionary (name, value, label)
    values
    ('Active', 0, 'OFF'),
    ('Active', 1, 'ON');
go
if 0 = (select count(*) from Data)
    insert into Data (name, ts, str_ts, value, str_value)
    values
    ('test-tag-1', '2020-01-01 00:00:00', '2020-01-01 00:00:00', 1, '1'),
    ('test-tag-1', '2020-02-01 00:00:00', '2020-02-01 00:00:00', 2, '2'),
    ('test-tag-1', '2020-03-01 00:00:00', '2020-03-01 00:00:00', 2, '2'),
    ('test-tag-1', '2020-04-01 00:00:00', '2020-04-01 00:00:00', 1, '1'),
    ('test-tag-1', '2020-05-01 00:00:00', '2020-05-01 00:00:00', 1, '1'),
    ('test-tag-5', '2020-01-01 00:00:00', '2020-01-01 00:00:00', NULL, 'A'),
    ('test-tag-5', '2020-02-01 00:00:00', '2020-02-01 00:00:00', NULL, 'B'),
    ('test-tag-5', '2020-03-01 00:00:00', '2020-03-01 00:00:00', NULL, 'B'),
    ('test-tag-5', '2020-04-01 00:00:00', '2020-04-01 00:00:00', NULL, 'A'),
    ('test-tag-5', '2020-05-01 00:00:00', '2020-05-01 00:00:00', NULL, 'A'),
    ('test-tag-6', '2020-01-01 00:00:00', '2020-01-01 00:00:00', 1, '1'),
    ('test-tag-6', '2020-02-01 00:00:00', '2020-02-01 00:00:00', 2, '2'),
    ('test-tag-6', '2020-03-01 00:00:00', '2020-03-01 00:00:00', 2, '2'),
    ('test-tag-6', '2020-04-01 00:00:00', '2020-04-01 00:00:00', 1, '1'),
    ('test-tag-6', '2020-05-01 00:00:00', '2020-05-01 00:00:00', 1, '1'),
    ('test-tag-7', '2020-01-01 00:00:00', '2020-01-01 00:00:00', NULL, NULL),
    ('test-tag-7', '2020-02-02 00:00:00', '2020-02-02 00:00:00', 1, '1');
go
