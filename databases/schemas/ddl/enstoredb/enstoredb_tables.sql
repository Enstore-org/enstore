--
-- Name: qa; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE qa (
    media_type character varying,
    library character varying,
    storage_group character varying,
    monthly numeric,
    weekly numeric,
    daily numeric,
    projected_daily numeric,
    quota integer,
    allocated integer,
    days_surviving numeric,
    alert character varying
);


ALTER TABLE public.qa OWNER TO enstore;

--
-- Name: tc; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE tc (
    media_type character varying,
    library character varying,
    storage_group character varying,
    volumes numeric
);

ALTER TABLE public.tc OWNER TO enstore;

--
-- Name: active_file_copying; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE active_file_copying (
    bfid character varying NOT NULL,
    remaining integer,
    "time" timestamp with time zone DEFAULT now()
);


ALTER TABLE public.active_file_copying OWNER TO enstore;

SET default_with_oids = true;

--
-- Name: bad_file; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE bad_file (
    bfid character varying,
    path character varying
);


ALTER TABLE public.bad_file OWNER TO enstore;

--
-- Name: volume; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE volume (
    id integer DEFAULT nextval(('volume_seq'::text)::regclass) NOT NULL,
    label character varying,
    block_size integer DEFAULT 0,
    capacity_bytes bigint DEFAULT 0,
    declared timestamp without time zone,
    eod_cookie character varying,
    first_access timestamp without time zone,
    last_access timestamp without time zone,
    library character varying,
    media_type character varying,
    non_del_files integer DEFAULT 0,
    remaining_bytes bigint DEFAULT 0,
    sum_mounts integer DEFAULT 0,
    sum_rd_access integer DEFAULT 0,
    sum_rd_err integer DEFAULT 0,
    sum_wr_access integer DEFAULT 0,
    sum_wr_err integer DEFAULT 0,
    system_inhibit_0 character varying,
    system_inhibit_1 character varying,
    si_time_0 timestamp without time zone,
    si_time_1 timestamp without time zone,
    user_inhibit_0 character varying,
    user_inhibit_1 character varying,
    storage_group character varying,
    file_family character varying,
    wrapper character varying,
    "comment" character varying,
    write_protected character(1) DEFAULT 'u'::bpchar,
    active_files integer DEFAULT 0,
    deleted_files integer DEFAULT 0,
    unknown_files integer DEFAULT 0,
    active_bytes bigint DEFAULT 0,
    deleted_bytes bigint DEFAULT 0,
    unknown_bytes bigint DEFAULT 0,
    modification_time timestamp without time zone
);


ALTER TABLE public.volume OWNER TO enstore;

--
-- Name: file; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE file (
    bfid character varying NOT NULL,
    crc bigint DEFAULT -1,
    deleted character(1) DEFAULT 'u'::bpchar,
    drive character varying,
    volume integer,
    location_cookie character varying,
    pnfs_path character varying,
    pnfs_id character varying,
    sanity_size bigint DEFAULT -1,
    sanity_crc bigint DEFAULT -1,
    size bigint DEFAULT 0,
    uid integer DEFAULT -1,
    gid integer DEFAULT -1,
    "update" timestamp without time zone
);


ALTER TABLE public.file OWNER TO enstore;

--
-- Name: file_copies_map; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE file_copies_map (
    bfid character varying,
    alt_bfid character varying
);


ALTER TABLE public.file_copies_map OWNER TO enstore;
--
-- Name: media_capacity; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE media_capacity (
    "type" character varying NOT NULL,
    capacity bigint
);


ALTER TABLE public.media_capacity OWNER TO enstore;

--
-- Name: migration; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE migration (
    src_bfid character varying,
    dst_bfid character varying,
    copied timestamp without time zone,
    swapped timestamp without time zone,
    checked timestamp without time zone,
    closed timestamp without time zone,
    remark character varying
);


ALTER TABLE public.migration OWNER TO enstore;

--
-- Name: migration_history; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE migration_history (
    src character varying NOT NULL,
    dst character varying NOT NULL,
    "time" timestamp without time zone DEFAULT now(),
    src_vol_id integer DEFAULT -1 NOT NULL,
    dst_vol_id integer DEFAULT -1 NOT NULL,
    closed_time timestamp without time zone
);


ALTER TABLE public.migration_history OWNER TO enstore;

--
-- Name: no_flipping_file_family; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE no_flipping_file_family (
    storage_group character varying,
    file_family character varying
);


ALTER TABLE public.no_flipping_file_family OWNER TO enstore;

--
-- Name: no_flipping_storage_group; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE no_flipping_storage_group (
    storage_group character varying
);


ALTER TABLE public.no_flipping_storage_group OWNER TO enstore;

--
-- Name: option; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE "option" (
    "key" character varying,
    value character varying
);


ALTER TABLE public."option" OWNER TO enstore;

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
-- Name: state; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE state (
    "time" timestamp without time zone DEFAULT now(),
    volume integer NOT NULL,
    "type" integer,
    value character varying
);


ALTER TABLE public.state OWNER TO enstore;

--
-- Name: state_type; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE state_type (
    id integer DEFAULT nextval(('state_type_seq'::text)::regclass) NOT NULL,
    name character varying NOT NULL
);


ALTER TABLE public.state_type OWNER TO enstore;
