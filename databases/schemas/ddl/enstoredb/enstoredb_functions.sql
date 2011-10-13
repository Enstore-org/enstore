--
-- Name: all_deleted(integer); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION all_deleted(integer) RETURNS integer
    AS $_$
begin
        if (select bfid from file where volume = $1 and deleted = 'n' limit 1) is null and
           (select bfid from file where volume = $1 and deleted = 'u' limit 1) is null and
           not (select bfid from file where volume = $1 and deleted = 'y' limit 1) is null
        then
                return 1;
        else
                return 0;
        end if;
end;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.all_deleted(integer) OWNER TO enstore;

--
-- Name: bytes_deleted(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_deleted(timestamp without time zone) RETURNS SETOF trb
    AS $_$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'DELETED' and
        state.volume = volume.id and
        time >= $1::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_deleted(timestamp without time zone) OWNER TO enstore;

--
-- Name: bytes_deleted(timestamp without time zone, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_deleted(timestamp without time zone, timestamp without time zone) RETURNS SETOF trb
    AS $_$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'DELETED' and
        state.volume = volume.id and
        time >= $1::timestamp and
        time < $2::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_deleted(timestamp without time zone, timestamp without time zone) OWNER TO enstore;

--
-- Name: bytes_deleted_last_7days(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_deleted_last_7days() RETURNS SETOF trb
    AS $$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'DELETED' and
        state.volume = volume.id and
        time >= current_date - 8 and
        time < current_date
group by storage_group
order by storage_group;
$$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_deleted_last_7days() OWNER TO enstore;

--
-- Name: bytes_recycled(timestamp without time zone, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_recycled(timestamp without time zone, timestamp without time zone) RETURNS SETOF trb
    AS $_$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'RECYCLED' and
        state.volume = volume.id and
        time >= $1::timestamp and
        time < $2::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_recycled(timestamp without time zone, timestamp without time zone) OWNER TO enstore;

--
-- Name: bytes_recycled(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_recycled(timestamp without time zone) RETURNS SETOF trb
    AS $_$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'RECYCLED' and
        state.volume = volume.id and
        time >= $1::timestamp
group by storage_group
order by storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_recycled(timestamp without time zone) OWNER TO enstore;

--
-- Name: bytes_recycled_last_7days(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION bytes_recycled_last_7days() RETURNS SETOF trb
    AS $$
select
        storage_group,
        sum(size)::bigint as recycled_bytes
from
        volume,
        state left join file on (file.volume = state.volume)
where
        value = 'RECYCLED' and
        state.volume = volume.id and
        time >= current_date - 8 and
        time < current_date
group by storage_group
order by storage_group;
$$
    LANGUAGE sql;


ALTER FUNCTION public.bytes_recycled_last_7days() OWNER TO enstore;

--
-- Name: get_media_type(character varying, bigint); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION get_media_type(character varying, bigint) RETURNS character varying
    AS $_$
DECLARE
MT VARCHAR;
BEGIN
         IF  $1 = '3480' and $2 = 107374182400 THEN
         MT = 'LTO1';
         ELSEIF  $1 = '3480' and $2 = 214748364800 THEN
         MT = 'LTO2';
         ELSEIF  $1 = '3480' and $2 < 100 THEN
         MT = NULL;
         ELSE
         MT=$1;
 END IF;
 return MT;
END;
$_$
    LANGUAGE plpgsql;


ALTER FUNCTION public.get_media_type(character varying, bigint) OWNER TO enstore;

--
-- Name: lookup_stype(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION lookup_stype(character varying) RETURNS integer
    AS $_$select id from state_type where name = $1;$_$
    LANGUAGE sql;


ALTER FUNCTION public.lookup_stype(character varying) OWNER TO enstore;

--
-- Name: lookup_vol(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION lookup_vol(character varying) RETURNS integer
    AS $_$select id from volume where label = $1;$_$
    LANGUAGE sql;


ALTER FUNCTION public.lookup_vol(character varying) OWNER TO enstore;

--
-- Name: plpgsql_call_handler(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION plpgsql_call_handler() RETURNS language_handler
    AS '$libdir/plpgsql', 'plpgsql_call_handler'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_call_handler() OWNER TO enstore;

--
-- Name: plpgsql_validator(oid); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION plpgsql_validator(oid) RETURNS void
    AS '$libdir/plpgsql', 'plpgsql_validator'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_validator(oid) OWNER TO enstore;

--
-- Name: quota_alert(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION quota_alert() RETURNS SETOF qa
    AS $$
select *,
	case
		when m1.days_surviving < 3 then 'less than 3 days'
		when m1.days_surviving < 7 then 'less than 1 week'
	end :: varchar as alert
	from
	(select *,
		trunc((m.quota - m.allocated)/(
			case
				when m.projected_daily = 0 then null
				else m.projected_daily
			end)) as days_surviving
	from
		(select
			u.media_type, u.library, u.storage_group, u.monthly,
			u.weekly, u.daily,
			case
				when u.weekly/7 > u.daily or u.daily is null
					then round(u.weekly/7, 5)
				else u.daily
			end as projected_daily,
			quota.quota, sg_count.count as allocated
		from quota, sg_count,
			(select monthly.media_type, monthly.library,
				monthly.storage_group, monthly.volumes as monthly,
				dw.weekly, dw.daily
			from
				tape_consumption('1 month', 'n') as monthly
				full outer join
				(select weekly.media_type, weekly.storage_group, daily.volumes as daily , weekly.volumes as weekly
				from
					tape_consumption('1 day', 'r') as daily
					full outer join
					tape_consumption('1 week', 'r') as weekly
					on (daily.media_type = weekly.media_type and daily.storage_group = weekly.storage_group)) as dw
				on (monthly.media_type = dw.media_type and monthly.storage_group = dw.storage_group)
			order by media_type, storage_group) as u
		where
			u.library = quota.library and
			u.storage_group = quota.storage_group and
			sg_count.library = u.library and
			sg_count.storage_group = u.storage_group) as m
	) as m1;
$$
    LANGUAGE sql;


ALTER FUNCTION public.quota_alert() OWNER TO enstore;

--
-- Name: set_update(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION set_update() RETURNS "trigger"
    AS $$
declare
begin
    new.update = now();
    return new;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.set_update() OWNER TO enstore;

--
-- Name: tape_consumption(interval, character); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION tape_consumption(interval, character) RETURNS SETOF tc
    AS $_$
select media_type, library, storage_group, sum(volumes)
from
	(select media_type, library, storage_group, file_family,
		sum(size),
		case
			when $2 = 'r' then
				round(sum(size)/media_capacity.capacity, 5)
			else
				trunc((sum(size)-1)/media_capacity.capacity)+1
		end as volumes
	from file, volume, media_capacity
	where
		file.volume = volume.id and
		media_capacity.type = volume.media_type and
		volume.media_type != 'null' and
		bfid > 'CDMS' || to_char(trunc(date_part('epoch', now() - $1)), 'FM99999999999999') || '00000' and
		bfid < 'CDMS' || to_char(trunc(date_part('epoch', now() + interval '1 day')), 'FM99999999999999') || '00000'
		group by media_type, library, storage_group,
			file_family, capacity) as foo
group by media_type, library, storage_group;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.tape_consumption(interval, character) OWNER TO enstore;

--
-- Name: tapes_recycled(timestamp without time zone, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION tapes_recycled(timestamp without time zone, timestamp without time zone) RETURNS SETOF tr
    AS $_$
select
        media_type,
        count(*)::int as recycled
from
        state,
        volume
where
        state.volume = volume.id and
        value = 'RECYCLED' and
        time >= $1::timestamp and
        time < $2::timestamp
group by media_type
order by media_type;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.tapes_recycled(timestamp without time zone, timestamp without time zone) OWNER TO enstore;

--
-- Name: tapes_recycled(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION tapes_recycled(timestamp without time zone) RETURNS SETOF tr
    AS $_$
select
        media_type,
        count(*)::int as recycled
from
        state,
        volume
where
        state.volume = volume.id and
        value = 'RECYCLED' and
        time >= $1::timestamp
group by media_type
order by media_type;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.tapes_recycled(timestamp without time zone) OWNER TO enstore;

--
-- Name: tapes_recycled_last_7days(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION tapes_recycled_last_7days() RETURNS SETOF tr
    AS $$
select
        media_type,
        count(*)::int as recycled
from
        state,
        volume
where
        state.volume = volume.id and
        value = 'RECYCLED' and
        time >= current_date - 8 and
        time < current_date
group by media_type
order by media_type;
$$
    LANGUAGE sql;


ALTER FUNCTION public.tapes_recycled_last_7days() OWNER TO enstore;

--
-- Name: update_volume_file_counters(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION update_volume_file_counters() RETURNS "trigger"
    AS $$
DECLARE
delta bigint;
BEGIN
IF(TG_OP='INSERT') THEN
	IF(NEW.deleted='u') THEN
		update volume set unknown_files=unknown_files+1, unknown_bytes=unknown_bytes+NEW.size,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
	ELSEIF (NEW.deleted='y') THEN
		update volume set deleted_files=deleted_files+1, deleted_bytes=deleted_bytes+NEW.size,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
	ELSEIF (NEW.deleted='n') THEN
		update volume set active_files=active_files+1, active_bytes=active_bytes+NEW.size,modification_time=LOCALTIMESTAMP(0)  where volume.id=NEW.volume;
	END IF;
ELSEIF (TG_OP='UPDATE') THEN
	delta := NEW.size-OLD.size;
	IF(NEW.deleted<>OLD.deleted) THEN
		IF(OLD.deleted='y') THEN
			update volume set deleted_files=deleted_files-1, deleted_bytes=deleted_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		ELSEIF (OLD.deleted='n') THEN
			update volume set active_files= active_files-1, active_bytes=active_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0)  where volume.id=NEW.volume;
		ELSEIF (OLD.deleted='u') THEN
			update volume set unknown_files= unknown_files-1, unknown_bytes=unknown_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		END IF;
		IF(NEW.deleted='u') THEN
			update volume set unknown_files= unknown_files+1, unknown_bytes=unknown_bytes+OLD.size+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		ELSEIF (NEW.deleted='y') THEN
			update volume set deleted_files= deleted_files+1, deleted_bytes=deleted_bytes+OLD.size+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		ELSEIF (NEW.deleted='n') THEN
			update volume set active_files= active_files+1, active_bytes=active_bytes+OLD.size+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		END IF;
	ELSEIF (OLD.size<>NEW.size) THEN
		IF(OLD.deleted='y') THEN
			update volume set deleted_bytes=deleted_bytes+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		ELSEIF (OLD.deleted='n') THEN
			update volume set active_bytes=active_bytes+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		ELSEIF (OLD.deleted='u') THEN
			update volume set unknown_bytes=unknown_bytes+delta,modification_time=LOCALTIMESTAMP(0) where volume.id=NEW.volume;
		END IF;
	END IF;
ELSEIF (TG_OP='DELETE') THEN
	IF(OLD.deleted='y') THEN
		update volume set  deleted_files=deleted_files-1, deleted_bytes=deleted_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0)  where volume.id=OLD.volume;
	ELSEIF (OLD.deleted='n') THEN
		update volume set active_files= active_files-1, active_bytes=active_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0) where volume.id=OLD.volume;
	ELSEIF (OLD.deleted='u') THEN
		update volume set unknown_files= unknown_files-1, unknown_bytes=unknown_bytes-OLD.size,modification_time=LOCALTIMESTAMP(0) where volume.id=OLD.volume;
	END IF;
	RETURN OLD;
END IF;
RETURN NEW;
END;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.update_volume_file_counters() OWNER TO enstore;

--
-- Name: populate file, files_in_transition; Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION populate_file_table() RETURNS "trigger"
    AS $$
BEGIN
IF(TG_OP='INSERT') THEN
        NEW.cache_status='CREATED';
	NEW.cache_mod_time=LOCALTIMESTAMP(0);
ELSEIF (TG_OP='UPDATE') THEN
        IF (OLD.cache_status<>NEW.cache_status) THEN
		NEW.cache_mod_time=LOCALTIMESTAMP(0);
	END IF;
        IF (OLD.archive_status<>NEW.archive_status) THEN
		NEW.archive_mod_time=LOCALTIMESTAMP(0);
	END IF;
	IF (NEW.deleted<>OLD.deleted) THEN
	   IF (OLD.deleted='n') THEN
	   	   IF (NEW.deleted='y' OR NEW.deleted='u') THEN
		      ---
		      --- Updating package counters
		      ---
		      BEGIN
	      	      	    update file set active_package_files_count=active_package_files_count-1 where bfid=OLD.package_id;
		      END;
	   	   END IF;
           ELSE
	   	   IF (NEW.deleted='n') THEN
		      ---
		      --- Updating package counters
		      ---
		      BEGIN
		      	    update file set active_package_files_count=active_package_files_count+1 where bfid=OLD.package_id;
		      END;
	   	   END IF;
	   END IF;
	END IF;
END IF;
RETURN NEW;
END;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.populate_file_table() OWNER TO enstore;

CREATE OR REPLACE FUNCTION populate_files_in_transition_table() RETURNS "trigger"
    AS $$
BEGIN
IF(TG_OP='INSERT') THEN
	BEGIN
		INSERT INTO files_in_transition values (NEW.bfid);
	END;
ELSEIF (TG_OP='UPDATE') THEN
	IF (OLD.bfid<>NEW.bfid) THEN
		BEGIN
			UPDATE files_in_transition set bfid=NEW.bfid where bfid=OLD.bfid;
		END;
	END IF;
	IF (NEW.archive_status = 'ARCHIVED') THEN
		BEGIN
			DELETE FROM files_in_transition WHERE bfid=NEW.bfid;
		END;
	END IF;

END IF;
RETURN NEW;
END;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.populate_files_in_transition_table() OWNER TO enstore;

--
-- Name: write_protect_status(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION write_protect_status(character varying) RETURNS character
    AS $_$
select
    case value
        when 'ON' then 'y'
        when 'OFF' then 'n'
        else 'u'
    end
from state, state_type, volume
where
    state.type = state_type.id and
    state_type.name = 'write_protect' and
    state.volume = volume.id and
    volume.label = $1
order by time desc limit 1;
$_$
    LANGUAGE sql;


ALTER FUNCTION public.write_protect_status(character varying) OWNER TO enstore;
