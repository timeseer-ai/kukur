create table Metadata (
    id serial,
    name text not null,
    description text
);

insert into Metadata (name, description)
values ('test-tag-1', 'test 1');
insert into Metadata (name, description)
values ('test-tag-2', 'test 2');


create table Data (
    id serial,
    name text not null,
    ts timestamp with time zone,
    value double precision
);

insert into Data (name, ts, value)
values ('test-tag-1', '2020-01-01T00:00:00Z', 2);
insert into Data (name, ts, value)
values ('test-tag-1', '2020-02-01T00:00:00Z', 3);
insert into Data (name, ts, value)
values ('test-tag-1', '2020-03-01T00:00:00Z', null);
