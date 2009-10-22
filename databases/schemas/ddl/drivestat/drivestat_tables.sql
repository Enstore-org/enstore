--
-- Name: status; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE status (
    drive_sn character varying(32),
    drive_vendor character varying(32),
    product_type character varying(32),
    host character varying(32),
    logical_drive_name character varying(32),
    stat_type character varying(32),
    "time" timestamp without time zone,
    tape_volser character varying(32),
    power_hrs integer,
    motion_hrs integer,
    cleaning_bit integer,
    mb_user_read integer,
    mb_user_write integer,
    mb_dev_read integer,
    mb_dev_write integer,
    read_errors integer,
    write_errors integer,
    track_retries integer,
    underrun integer,
    mount_count integer,
    wp integer DEFAULT 0,
    firmware_version character varying(32)
);


ALTER TABLE public.status OWNER TO enstore;
