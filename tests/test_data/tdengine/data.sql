create database kukur;
use kukur;

create stable data (ts timestamp, v double) tags (name binary(64), location binary(64));

create table `test-tag-1` using data (name, location) tags ('test-tag-1', 'Antwerp');
insert into `test-tag-1` values ('2020-01-01T00:00:00Z', 1);
insert into `test-tag-1` values ('2020-01-02T00:00:00Z', 2);
insert into `test-tag-1` values ('2020-01-03T00:00:00Z', 2);
insert into `test-tag-1` values ('2020-01-04T00:00:00Z', 1);
insert into `test-tag-1` values ('2020-01-05T00:00:00Z', 1);

create table `test-tag-2` using data (name, location) tags ('test-tag-2', 'Barcelona');
insert into `test-tag-2` values ('2020-01-01T00:00:00Z', -6);
insert into `test-tag-2` values ('2020-01-02T00:00:00Z', -7);
insert into `test-tag-2` values ('2020-01-03T00:00:00Z', -8);
insert into `test-tag-2` values ('2020-01-04T00:00:00Z', -9);
insert into `test-tag-2` values ('2020-01-05T00:00:00Z', -10);
