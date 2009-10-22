--
-- Name: encp_xfer_by_day_view; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW encp_xfer_by_day_view AS
    SELECT (sum(encp_xfer.size))::double precision AS date, encp_xfer.rw AS size, encp_xfer.mover AS rw, (substr((encp_xfer.date)::text, 0, 11))::timestamp without time zone AS mover FROM encp_xfer GROUP BY encp_xfer.rw, encp_xfer.mover, substr((encp_xfer.date)::text, 0, 11);

ALTER TABLE public.encp_xfer_by_day_view OWNER TO enstore;

--
-- Name: enstore_tables; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW enstore_tables AS
    SELECT n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner, c.reltuples AS "rows", c.relpages AS pages, t.spcname AS "tablespace", c.relhaspkey AS haspkey, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, (c.reltriggers > 0) AS hastriggers FROM ((pg_class c LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace))) LEFT JOIN pg_tablespace t ON ((t.oid = c.reltablespace))) WHERE ((c.relkind = 'r'::"char") AND (n.nspname = 'public'::name));


ALTER TABLE public.enstore_tables OWNER TO enstore;

--
-- Name: lock_status; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW lock_status AS
    SELECT (SELECT pg_class.relname FROM pg_class WHERE (pg_class.oid = pg_locks.relation)) AS "table", (SELECT pg_database.datname FROM pg_database WHERE (pg_database.oid = pg_locks."database")) AS "database", pg_locks."transaction", (SELECT pg_stat_activity.usename FROM pg_stat_activity WHERE (pg_stat_activity.procpid = pg_locks.pid)) AS "user", pg_locks."mode", pg_locks."granted" FROM pg_locks;


ALTER TABLE public.lock_status OWNER TO enstore;


