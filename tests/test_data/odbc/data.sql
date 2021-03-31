declare @name nvarchar(max);
declare @startdate datetime2;
declare @enddate datetime2;

set @name = ?;
set @startdate = ?;
set @enddate = ?;

if @name = 'test-tag-5'
    select ts, str_value from Data where name = @name and ts between @startdate and @enddate;
else
    select ts, value from Data where name = @name and ts between @startdate and @enddate;
