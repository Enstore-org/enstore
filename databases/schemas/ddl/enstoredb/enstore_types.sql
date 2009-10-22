
CREATE TYPE tr AS (
	media_type character varying,
	recycled integer
);


ALTER TYPE public.tr OWNER TO enstore;

--
-- Name: trb; Type: TYPE; Schema: public; Owner: enstore
--

CREATE TYPE trb AS (
	storage_group character varying,
	recycled_bytes bigint
);

ALTER TYPE public.trb OWNER TO enstore;