
SET default_tablespace = '';

SET default_with_oids = true;

--
-- Name: blanks; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE blanks (
    date timestamp without time zone NOT NULL,
    media_type character varying NOT NULL,
    blanks integer
);


ALTER TABLE public.blanks OWNER TO enstore;

--
-- Name: xfer_count_by_day; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE xfer_count_by_day (
    date date,
    storage_group character varying,
    n_read bigint,
    n_write bigint
);


ALTER TABLE public.xfer_count_by_day OWNER TO enstore;


CREATE TABLE xfer_by_day (
    date date NOT NULL,
    storage_group character varying NOT NULL,
    "read" bigint,
    "write" bigint,
    n_read bigint,
    n_write bigint
);


ALTER TABLE public.xfer_by_day OWNER TO enstore;

--
-- Name: xfer_by_day_by_mover; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE xfer_by_day_by_mover (
    date date NOT NULL,
    mover character varying NOT NULL,
    "read" bigint,
    "write" bigint
);


ALTER TABLE public.xfer_by_day_by_mover OWNER TO enstore;

--
-- Name: drive_utilization; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE drive_utilization (
    "time" timestamp with time zone NOT NULL,
    "type" character varying NOT NULL,
    total integer,
    busy integer,
    tape_library character varying DEFAULT 'TBA'::character varying NOT NULL,
    storage_group character varying
);


ALTER TABLE public.drive_utilization OWNER TO enstore;

SET default_with_oids = true;

--
-- Name: encp_error; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE encp_error (
    date timestamp without time zone NOT NULL,
    node character varying NOT NULL,
    pid integer NOT NULL,
    username character varying NOT NULL,
    encp_id character varying,
    version character varying NOT NULL,
    "type" character varying NOT NULL,
    error character varying NOT NULL,
    src character varying,
    dst character varying,
    size bigint,
    storage_group character varying,
    file_family character varying,
    wrapper character varying,
    mover character varying,
    drive_id character varying,
    drive_sn character varying,
    rw character(1),
    volume character varying,
    library character varying
);


ALTER TABLE public.encp_error OWNER TO enstore;

--
-- Name: encp_xfer; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE encp_xfer (
    date timestamp without time zone NOT NULL,
    node character varying(32) NOT NULL,
    pid integer NOT NULL,
    username character varying(32) NOT NULL,
    src text NOT NULL,
    dst text NOT NULL,
    size bigint NOT NULL,
    rw character(1) NOT NULL,
    overall_rate bigint NOT NULL,
    network_rate bigint NOT NULL,
    drive_rate bigint NOT NULL,
    volume character varying(16) NOT NULL,
    mover character varying(32) NOT NULL,
    drive_id character varying(16) NOT NULL,
    drive_sn character varying(16) NOT NULL,
    elapsed double precision NOT NULL,
    media_changer character varying(32) NOT NULL,
    mover_interface character varying(32) NOT NULL,
    driver character varying(16) NOT NULL,
    storage_group character varying(16) NOT NULL,
    encp_ip character varying(16) NOT NULL,
    encp_id character varying(64) NOT NULL,
    disk_rate bigint,
    transfer_rate bigint,
    encp_version character varying(48),
    file_family character varying,
    wrapper character varying,
    library character varying
);


ALTER TABLE public.encp_xfer OWNER TO enstore;

--
-- Name: encp_xfer_average_by_storage_group; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE encp_xfer_average_by_storage_group (
    unix_time integer NOT NULL,
    date timestamp without time zone NOT NULL,
    from_date timestamp without time zone NOT NULL,
    to_date timestamp without time zone NOT NULL,
    storage_group character varying(16) NOT NULL,
    rw character(1) NOT NULL,
    avg_overall_rate double precision,
    avg_network_rate double precision,
    avg_disk_rate double precision,
    avg_transfer_rate double precision,
    avg_drive_rate double precision,
    avg_size double precision,
    stddev_overall_rate double precision,
    stddev_network_rate double precision,
    stddev_disk_rate double precision,
    stddev_transfer_rate double precision,
    stddev_drive_rate double precision,
    stddev_size double precision,
    counter integer
);


ALTER TABLE public.encp_xfer_average_by_storage_group OWNER TO enstore;

--
-- Name: event; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE event (
    tag character varying(48) NOT NULL,
    name character varying(64) NOT NULL,
    node character varying(32) NOT NULL,
    username character varying(32) NOT NULL,
    "start" timestamp without time zone NOT NULL,
    finish timestamp without time zone,
    status integer,
    "comment" character varying(64)
);


ALTER TABLE public.event OWNER TO enstore;

--
-- Name: mover; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE mover (
    name character varying NOT NULL,
    max_rate double precision,
    max_buffer bigint,
    check_written_file integer,
    max_failures integer,
    library character varying,
    host character varying,
    max_consecutive_failures integer,
    max_time_in_state integer,
    device character varying,
    norestart character varying,
    driver character varying,
    port integer,
    send_stats integer,
    mc_device character varying,
    compression integer,
    statistics_path character varying,
    mount_delay integer,
    check_first_written_file integer,
    data_ip character varying,
    logname character varying,
    media_changer character varying,
    hostip character varying,
    do_cleaning character varying,
    update_interval integer,
    syslog_entry character varying,
    min_buffer bigint,
    noupdown character varying,
    inq_ignore character varying,
    dismount_delay integer,
    max_dismount_delay integer,
    max_in_state_cnt integer,
    include_thread_name character varying
);


ALTER TABLE public.mover OWNER TO enstore;

SET default_with_oids = true;

--
-- Name: old_bytes_per_day; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE old_bytes_per_day (
    date date,
    "read" bigint,
    "write" bigint
);


ALTER TABLE public.old_bytes_per_day OWNER TO enstore;

--
-- Name: quota; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE quota (
    library character varying NOT NULL,
    storage_group character varying NOT NULL,
    requested integer DEFAULT 0,
    authorized integer DEFAULT 0,
    quota integer DEFAULT 0,
    significance character(1) DEFAULT 'y'::bpchar
);


ALTER TABLE public.quota OWNER TO enstore;

SET default_with_oids = false;

--
-- Name: quotas; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE quotas (
    "time" timestamp with time zone DEFAULT now() NOT NULL,
    library character varying NOT NULL,
    storage_group character varying NOT NULL,
    allocated integer,
    blank integer NOT NULL,
    written integer NOT NULL,
    requested integer,
    authorized integer,
    quota integer
);


ALTER TABLE public.quotas OWNER TO enstore;

--
-- Name: rate; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE rate (
    "time" timestamp with time zone NOT NULL,
    "read" bigint,
    "write" bigint,
    read_null bigint,
    write_null bigint
);


ALTER TABLE public.rate OWNER TO enstore;

--
-- Name: tape_library_slots_usage; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE tape_library_slots_usage (
    "time" timestamp with time zone NOT NULL,
    tape_library character varying NOT NULL,
    "location" character varying NOT NULL,
    media_type character varying,
    total integer,
    free integer,
    used integer,
    disabled integer
);


ALTER TABLE public.tape_library_slots_usage OWNER TO enstore;

SET default_with_oids = true;

--
-- Name: tape_mounts; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE tape_mounts (
    node character varying(32) NOT NULL,
    volume character varying(16) NOT NULL,
    "type" character varying(32) NOT NULL,
    logname character varying(16) NOT NULL,
    "start" timestamp without time zone NOT NULL,
    finish timestamp without time zone,
    state character(1) NOT NULL
);


ALTER TABLE public.tape_mounts OWNER TO enstore;

--
-- Name: tape_mounts_tmp; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE tape_mounts_tmp (
    volume character varying(16) NOT NULL,
    state character varying(1) NOT NULL,
    id bigint
);


ALTER TABLE public.tape_mounts_tmp OWNER TO enstore;

SET default_with_oids = false;

--
-- Name: time; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE "time" (
    date_part double precision
);


ALTER TABLE public."time" OWNER TO enstore;

SET default_with_oids = true;

--
-- Name: tmp_xfer_by_day; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE tmp_xfer_by_day (
    date date NOT NULL,
    storage_group character varying NOT NULL,
    "read" bigint,
    "write" bigint
);


ALTER TABLE public.tmp_xfer_by_day OWNER TO enstore;

--
-- Name: write_protect_summary; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE write_protect_summary (
    date timestamp without time zone DEFAULT now() NOT NULL,
    total integer,
    should integer,
    not_yet integer,
    done integer
);


ALTER TABLE public.write_protect_summary OWNER TO enstore;

--
-- Name: write_protect_summary_by_library; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE write_protect_summary_by_library (
    date timestamp without time zone DEFAULT now() NOT NULL,
    library character varying NOT NULL,
    total integer,
    should integer,
    not_yet integer,
    done integer
);


ALTER TABLE public.write_protect_summary_by_library OWNER TO enstore;

--
-- Name: xfer_by_month; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE xfer_by_month (
    date date NOT NULL,
    storage_group character varying NOT NULL,
    "read" bigint,
    "write" bigint
);


ALTER TABLE public.xfer_by_month OWNER TO enstore;

