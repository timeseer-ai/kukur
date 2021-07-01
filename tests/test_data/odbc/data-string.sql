set NOCOUNT on;

declare @name nvarchar(max);
declare @startdate datetime2;
declare @enddate datetime2;

set @startdate = '{1}';
set @enddate = '{2}';
set @name = '{0}';

if @name = 'test-tag-5'
    select ts, str_value from Data where name = @name and ts >= @startdate and ts < @enddate;
else
    select ts, value from Data where name = @name and ts >= @startdate and ts < @enddate;
