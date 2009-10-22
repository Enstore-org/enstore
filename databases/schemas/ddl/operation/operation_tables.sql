--
-- Name: job; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE job (
    id integer DEFAULT nextval(('job_id'::text)::regclass) NOT NULL,
    name character varying NOT NULL,
    "type" integer NOT NULL,
    "start" timestamp without time zone DEFAULT now(),
    finish timestamp without time zone,
    "comment" character varying
);


ALTER TABLE public.job OWNER TO enstore;

--
-- Name: job_definition; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE job_definition (
    id integer DEFAULT nextval(('job_definition_id'::text)::regclass) NOT NULL,
    name character varying NOT NULL,
    tasks integer DEFAULT 0,
    remarks character varying
);


ALTER TABLE public.job_definition OWNER TO enstore;

--
-- Name: object; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE "object" (
    job integer NOT NULL,
    "object" character varying,
    association character varying
);


ALTER TABLE public."object" OWNER TO enstore;

--
-- Name: progress; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE progress (
    job integer NOT NULL,
    task integer NOT NULL,
    "start" timestamp without time zone DEFAULT now(),
    finish timestamp without time zone,
    "comment" character varying,
    args character varying,
    result character varying
);


ALTER TABLE public.progress OWNER TO enstore;

--
-- Name: task; Type: TABLE; Schema: public; Owner: enstore; Tablespace: 
--

CREATE TABLE task (
    id integer DEFAULT nextval(('task_id'::text)::regclass) NOT NULL,
    seq integer NOT NULL,
    job_type integer NOT NULL,
    dsc character varying,
    "action" character varying,
    "comment" character varying,
    auto_start character(1) DEFAULT 'm'::bpchar
);


ALTER TABLE public.task OWNER TO enstore;

