
--
-- Name: bd; Type: TYPE; Schema: public; Owner: enstore
--

CREATE TYPE bd AS (
	media_type character varying,
	blanks_drawn integer
);


ALTER TYPE public.bd OWNER TO enstore;

--
-- Name: daily_xfer_size; Type: TYPE; Schema: public; Owner: enstore
--

CREATE TYPE daily_xfer_size AS (
	date date,
	storage_group character varying,
	"read" bigint,
	"write" bigint
);


ALTER TYPE public.daily_xfer_size OWNER TO enstore;

--
-- Name: ts; Type: TYPE; Schema: public; Owner: enstore
--

CREATE TYPE ts AS (
	storage_group character varying,
	total bigint,
	"read" bigint,
	"write" bigint
);


ALTER TYPE public.ts OWNER TO enstore;
