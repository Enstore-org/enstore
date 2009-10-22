
--
-- Name: blanks_drawn(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION blanks_drawn(date) RETURNS SETOF bd
    AS $_$
select
    a.media_type, a.blanks - b.blanks as blanks_drawn
from
    blanks_of_day($1::date) a,
    blanks_of_day(current_date) b
where
    a.media_type = b.media_type;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.blanks_drawn(date) OWNER TO enstore;

--
-- Name: blanks_drawn(date, date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION blanks_drawn(date, date) RETURNS SETOF bd
    AS $_$
select
    a.media_type, a.blanks - b.blanks as blanks_drawn
from
    blanks_of_day($1::date) a,
    blanks_of_day($2::date) b
 where
    a.media_type = b.media_type;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.blanks_drawn(date, date) OWNER TO enstore;

--
-- Name: blanks_drawn_last_7days(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION blanks_drawn_last_7days() RETURNS SETOF bd
    AS $$
select
    a.media_type, a.blanks - b.blanks as blanks_drawn
from
    blanks_of_day(current_date - 7) a,
    blanks_of_day(current_date) b
where
    a.media_type = b.media_type;
 $$
    LANGUAGE sql;


ALTER FUNCTION public.blanks_drawn_last_7days() OWNER TO enstore;

SET default_tablespace = '';

SET default_with_oids = true;

--
-- Name: blanks_of_day(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION blanks_of_day(date) RETURNS SETOF blanks
    AS $_$
select
    *
from
    blanks
where
    date = (select min(date) from blanks where date >= $1::date and date < $1::date + '1 day'::interval);
$_$
    LANGUAGE sql;


ALTER FUNCTION public.blanks_of_day(date) OWNER TO enstore;

CREATE OR REPLACE FUNCTION daily_count(date) RETURNS SETOF xfer_count_by_day
    AS $_$
select
    $1::date,
    storage_group,
    sum(case when rw='r' then 1 else 0 end)::bigint as n_read,
    sum(case when rw='w' then 1 else 0 end)::bigint as n_write
from
    encp_xfer
where
    date >= $1 and date < $1 + 1
group by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.daily_count(date) OWNER TO enstore;


--
-- Name: daily_size(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION daily_size(date) RETURNS SETOF xfer_by_day
    AS $_$
select
    $1::date,
    storage_group,
    sum(case when rw='r' then size else 0 end)::bigint as read,
    sum(case when rw='w' then size else 0 end)::bigint as write,
    sum(case when rw='r' then 1 else 0 end)::bigint as n_read,
    sum(case when rw='w' then 1 else 0 end)::bigint as n_write
from
    encp_xfer
where
    date >= $1 and date < $1 + 1
group by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.daily_size(date) OWNER TO enstore;

--
-- Name: daily_size_by_mover(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION daily_size_by_mover(date) RETURNS SETOF xfer_by_day_by_mover
    AS $_$
select
    $1::date,
    rtrim(mover, '.mover') as mover,
    sum(case when rw='r' then size else 0 end)::bigint as read,
    sum(case when rw='w' then size else 0 end)::bigint as write
from
    encp_xfer
where
    date >= $1 and date < $1 + 1
group by rtrim(mover, '.mover');
$_$
    LANGUAGE sql;


ALTER FUNCTION public.daily_size_by_mover(date) OWNER TO enstore;

--
-- Name: data_transfer(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION data_transfer(timestamp without time zone) RETURNS SETOF ts
    AS $_$
select
        storage_group,
        sum(size)::bigint as total,
        sum(case
                when rw = 'r' then size
                else 0
        end)::bigint as read,
        sum(case
                when rw = 'w' then size
                else 0
        end)::bigint as write
from
        encp_xfer
where
        date >= $1::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.data_transfer(timestamp without time zone) OWNER TO enstore;

--
-- Name: data_transfer(timestamp without time zone, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION data_transfer(timestamp without time zone, timestamp without time zone) RETURNS SETOF ts
    AS $_$
select
        storage_group,
        sum(size)::bigint as total,
        sum(case
                when rw = 'r' then size
                else 0
        end)::bigint as read,
        sum(case
                when rw = 'w' then size
                else 0
        end)::bigint as write
from
        encp_xfer
where
        date >= $1::timestamp and date < $2::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.data_transfer(timestamp without time zone, timestamp without time zone) OWNER TO enstore;

--
-- Name: data_transfer_last_7days(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION data_transfer_last_7days() RETURNS SETOF ts
    AS $$
select
        storage_group,
        sum(size)::bigint as total,
        sum(case
                when rw = 'r' then size
                else 0
        end)::bigint as read,
        sum(case
                when rw = 'w' then size
                else 0
        end)::bigint as write
from
        encp_xfer
where
        date >= current_date - 8 and
        date < current_date
group by storage_group
order by storage_group;
$$
    LANGUAGE sql;


ALTER FUNCTION public.data_transfer_last_7days() OWNER TO enstore;

--
-- Name: make_daily_xfer_count(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_daily_xfer_count() RETURNS date
    AS $$
declare
	day date;
begin
	day = (select max(date) from xfer_count_by_day);
	if day is null then
		day = (select make_first_daily_xfer_count())::date;
	end if;
	if day is null then
		return day;
	end if;
	day = day + 1;
	while day < current_date loop
		insert into xfer_count_by_day select * from daily_count(day);
		day = day + 1;
	end loop;
	return day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_daily_xfer_count() OWNER TO enstore;

--
-- Name: make_daily_xfer_size(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_daily_xfer_size() RETURNS date
    AS $$
declare
	day date;
begin
	day = (select max(date) from xfer_by_day);
	if day is null then
		day = (select make_first_daily_xfer_size())::date;
	end if;
	if day is null then
		return day;
	end if;
	day = day + 1;
	while day < current_date loop
		insert into xfer_by_day select * from daily_size(day);
		day = day + 1;
	end loop;
	return day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_daily_xfer_size() OWNER TO enstore;

--
-- Name: make_daily_xfer_size(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_daily_xfer_size(date) RETURNS date
    AS $_$
declare
	day date;
begin
	day = (select max(date) from xfer_by_day);
	if day is null then
		day = (select make_first_daily_xfer_size())::date;
	end if;
	if day is null then
		return day;
	end if;
	day = day + 1;
	while day < $1::date loop
		insert into xfer_by_day select * from daily_size(day);
		day = day + 1;
	end loop;
	return day;
end;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_daily_xfer_size(date) OWNER TO enstore;

--
-- Name: make_daily_xfer_size_by_mover(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_daily_xfer_size_by_mover(date) RETURNS date
    AS $_$
declare
	day date;
begin
	day = (select max(date) from xfer_by_day_by_mover);
	if day is null then
		day = (select make_first_daily_size_by_mover())::date;
	end if;
	if day is null then
		return day;
	end if;
	day = day + 1;
	while day < $1::date loop
		insert into xfer_by_day_by_mover select * from daily_size_by_mover(day);
		day = day + 1;
	end loop;
	return day;
end;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_daily_xfer_size_by_mover(date) OWNER TO enstore;

--
-- Name: make_daily_xfer_size_by_mover(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_daily_xfer_size_by_mover() RETURNS date
    AS $$
declare
	day date;
begin
	day = (select max(date) from xfer_by_day_by_mover);
	if day is null then
		day = (select make_first_daily_size_by_mover())::date;
	end if;
	if day is null then
		return day;
	end if;
	day = day + 1;
	while day < current_date loop
		insert into xfer_by_day_by_mover select * from daily_size_by_mover(day);
		day = day + 1;
	end loop;
	return day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_daily_xfer_size_by_mover() OWNER TO enstore;

--
-- Name: make_first_daily_size_by_mover(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_first_daily_size_by_mover() RETURNS date
    AS $$
declare
        first_day date;
begin
        first_day = (select min(date)::date from encp_xfer);
	if first_day = current_date then
		return null;
	end if;
        if not first_day is null then
                insert into xfer_by_day_by_mover select * from daily_size_by_mover(first_day);
        end if;
        return first_day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_first_daily_size_by_mover() OWNER TO enstore;

--
-- Name: make_first_daily_xfer_count(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_first_daily_xfer_count() RETURNS date
    AS $$
declare
	first_day date;
begin
	first_day = (select min(date)::date from encp_xfer);
	if first_day = current_date then
		return null;
	end if;
	if not first_day is null then
		insert into xfer_count_by_day select * from daily_count(first_day);
	end if;
	return first_day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_first_daily_xfer_count() OWNER TO enstore;

--
-- Name: make_first_daily_xfer_size(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_first_daily_xfer_size() RETURNS date
    AS $$
declare
	first_day date;
begin
	first_day = (select min(date)::date from encp_xfer);
	if first_day = current_date then
		return null;
	end if;
	if not first_day is null then
		insert into xfer_by_day select * from daily_size(first_day);
	end if;
	return first_day;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_first_daily_xfer_size() OWNER TO enstore;

--
-- Name: make_first_monthly_xfer_size(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_first_monthly_xfer_size() RETURNS date
    AS $$
declare
	first_month date;
begin
	first_month = (select year_month((select min(date) from encp_xfer)));
	if first_month = (select year_month(current_date)) then
		return null;
	end if;
	if not first_month is null then
		insert into xfer_by_month select * from monthly_size(first_month);
	end if;
	return first_month;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_first_monthly_xfer_size() OWNER TO enstore;

--
-- Name: make_monthly_xfer_size(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_monthly_xfer_size(date) RETURNS date
    AS $_$
declare
	month date;
begin
	month = (select max(date) from xfer_by_month);
	if month is null then
		month = (select make_first_monthly_xfer_size());
	end if;
	if month is null then
		return month;
	end if;
	month = month + '1 mons'::interval;
	while month < year_month($1::date) loop
		insert into xfer_by_month select * from monthly_size(month);
		month = month + '1 mons'::interval;
	end loop;
	return month;
end;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_monthly_xfer_size(date) OWNER TO enstore;

--
-- Name: make_monthly_xfer_size(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION make_monthly_xfer_size() RETURNS date
    AS $$
declare
	month date;
begin
	month = (select max(date) from xfer_by_month);
	if month is null then
		month = (select make_first_monthly_xfer_size());
	end if;
	if month is null then
		return month;
	end if;
	month = month + '1 mons'::interval;
	while month < year_month(current_date) loop
		insert into xfer_by_month select * from monthly_size(month);
		month = month + '1 mons'::interval;
	end loop;
	return month;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.make_monthly_xfer_size() OWNER TO enstore;

--
-- Name: monthly_size(date); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION monthly_size(date) RETURNS SETOF daily_xfer_size
    AS $_$
select
    year_month(date) as date,
    storage_group,
    sum(case when rw='r' then size else 0 end)::bigint as read,
    sum(case when rw='w' then size else 0 end)::bigint as write
from
    encp_xfer
where
    date >= year_month($1) and date < year_month($1) + '1 mons'::interval
group by year_month(date), storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.monthly_size(date) OWNER TO enstore;

--
-- Name: plpgsql_call_handler(); Type: FUNCTION; Schema: public; Owner: products
--

CREATE OR REPLACE FUNCTION plpgsql_call_handler() RETURNS language_handler
    AS '$libdir/plpgsql', 'plpgsql_call_handler'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_call_handler() OWNER TO products;

--
-- Name: plpgsql_validator(oid); Type: FUNCTION; Schema: public; Owner: products
--

CREATE OR REPLACE FUNCTION plpgsql_validator(oid) RETURNS void
    AS '$libdir/plpgsql', 'plpgsql_validator'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_validator(oid) OWNER TO products;

--
-- Name: unix2timestamp(bigint); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION unix2timestamp(bigint) RETURNS timestamp with time zone
    AS $_$
DECLARE
    STAMP  TIMESTAMP WITH TIME ZONE;
BEGIN
    select TIMESTAMP WITH TIME ZONE 'epoch' +($1) * INTERVAL '1 second' into STAMP;
    return STAMP;
END;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.unix2timestamp(bigint) OWNER TO enstore;

--
-- Name: year_month(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION year_month(timestamp without time zone) RETURNS date
    AS $_$
select to_date(to_char($1, 'YYYY-MM-01'), 'YYYY-MM-DD');$_$
    LANGUAGE sql;


ALTER FUNCTION public.year_month(timestamp without time zone) OWNER TO enstore;

