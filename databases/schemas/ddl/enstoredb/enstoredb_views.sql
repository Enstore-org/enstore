
--
-- Name: cms_volume_with_only_deleted_files; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW cms_volume_with_only_deleted_files AS
    SELECT volume.label, volume.storage_group, volume.file_family, volume.system_inhibit_1 AS state, volume.media_type, volume.library FROM volume WHERE (((((volume.storage_group)::text = 'cms'::text) AND ((volume.system_inhibit_0)::text = 'none'::text)) AND ((volume.active_files + volume.unknown_files) = 0)) AND (volume.deleted_files > 0));


ALTER TABLE public.cms_volume_with_only_deleted_files OWNER TO enstore;

--
-- Name: duplicates; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW duplicates AS
    SELECT m.bfid AS "primary", m.alt_bfid AS "copy", f.deleted, f.pnfs_path FROM file_copies_map m, file f WHERE ((f.bfid)::text = (m.bfid)::text);


ALTER TABLE public.duplicates OWNER TO enstore;

--
-- Name: enstore_tables; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW enstore_tables AS
    SELECT n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner, c.reltuples AS "rows", c.relpages AS pages, t.spcname AS "tablespace", c.relhaspkey AS haspkey, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, (c.reltriggers > 0) AS hastriggers FROM ((pg_class c LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace))) LEFT JOIN pg_tablespace t ON ((t.oid = c.reltablespace))) WHERE ((c.relkind = 'r'::"char") AND (n.nspname = 'public'::name));


ALTER TABLE public.enstore_tables OWNER TO enstore;

--
-- Name: file2; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW file2 AS
    SELECT file.bfid, file.crc, file.deleted, file.drive, file.volume, file.location_cookie, file.pnfs_path, file.pnfs_id, file.sanity_size, file.sanity_crc, file.size, file.uid, file.gid, CASE WHEN (file.deleted = 'y'::bpchar) THEN 1 ELSE 0 END AS n_deleted, CASE WHEN (file.deleted = 'y'::bpchar) THEN file.size ELSE (0)::bigint END AS size_deleted, CASE WHEN (file.deleted = 'n'::bpchar) THEN 1 ELSE 0 END AS n_active, CASE WHEN (file.deleted = 'n'::bpchar) THEN file.size ELSE (0)::bigint END AS size_active, CASE WHEN ((file.deleted <> 'y'::bpchar) AND (file.deleted <> 'n'::bpchar)) THEN 1 ELSE 0 END AS n_unknown, CASE WHEN ((file.deleted <> 'y'::bpchar) AND (file.deleted <> 'n'::bpchar)) THEN file.size ELSE (0)::bigint END AS size_unknown FROM file;


ALTER TABLE public.file2 OWNER TO enstore;

--
-- Name: file_info; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW file_info AS
    SELECT f.bfid, f.crc AS complete_crc, CASE WHEN (f.deleted = 'y'::bpchar) THEN 'deleted'::text WHEN (f.deleted = 'n'::bpchar) THEN 'active'::text ELSE 'unknown'::text END AS deleted, v.label AS external_label, f.location_cookie, v.storage_group, v.file_family, v.library, v.media_type, f.size, f.gid, f.uid, f.pnfs_id AS pnfsid, f.pnfs_path AS pnfs_name0, f.sanity_size, f.sanity_crc, f.drive, f."update" FROM file f, volume v WHERE (f.volume = v.id);


ALTER TABLE public.file_info OWNER TO enstore;

--
-- Name: lock_status; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW lock_status AS
    SELECT (SELECT pg_class.relname FROM pg_class WHERE (pg_class.oid = pg_locks.relation)) AS "table", (SELECT pg_database.datname FROM pg_database WHERE (pg_database.oid = pg_locks."database")) AS "database", pg_locks."transaction", (SELECT pg_stat_activity.usename FROM pg_stat_activity WHERE (pg_stat_activity.procpid = pg_locks.pid)) AS "user", pg_locks."mode", pg_locks."granted" FROM pg_locks;


ALTER TABLE public.lock_status OWNER TO enstore;
--
-- Name: remaining_blanks; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW remaining_blanks AS
    SELECT volume.media_type, count(*) AS blanks FROM volume WHERE ((((volume.storage_group)::text = 'none'::text) AND ((volume.file_family)::text = 'none'::text)) AND ((volume.wrapper)::text = 'none'::text)) GROUP BY volume.media_type;


ALTER TABLE public.remaining_blanks OWNER TO enstore;

CREATE OR REPLACE VIEW  sg_count AS SELECT library, storage_group, count(*) from volume where not label LIKE '%.deleted' GROUP BY library, storage_group;

ALTER TABLE public.sg_count OWNER TO enstore;

--
-- Name: volume_summary; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW volume_summary AS
    SELECT volume.label, volume.capacity_bytes, sum(file2.size) AS used, sum(file2.n_active) AS n_active, sum(file2.size_active) AS active, sum(file2.n_deleted) AS n_deleted, sum(file2.size_deleted) AS deleted, sum(file2.n_unknown) AS n_unknown, sum(file2.size_unknown) AS "unknown", volume.remaining_bytes, (((volume.capacity_bytes - volume.remaining_bytes))::numeric - sum(file2.size)) AS unaccountable, volume.declared, volume.eod_cookie, volume.first_access, volume.last_access, volume.library, volume.media_type, volume.sum_mounts, volume.sum_rd_access, volume.sum_rd_err, volume.sum_wr_access, volume.sum_wr_err, volume.system_inhibit_0, volume.system_inhibit_1, volume.si_time_0, volume.si_time_1, volume.user_inhibit_0, volume.user_inhibit_1, volume.storage_group, volume.file_family, volume.wrapper, volume."comment" FROM file2, volume WHERE (file2.volume = volume.id) GROUP BY volume.label, volume.capacity_bytes, volume.remaining_bytes, volume.declared, volume.eod_cookie, volume.first_access, volume.last_access, volume.library, volume.media_type, volume.sum_mounts, volume.sum_rd_access, volume.sum_rd_err, volume.sum_wr_access, volume.sum_wr_err, volume.system_inhibit_0, volume.system_inhibit_1, volume.si_time_0, volume.si_time_1, volume.user_inhibit_0, volume.user_inhibit_1, volume.storage_group, volume.file_family, volume.wrapper, volume."comment";


ALTER TABLE public.volume_summary OWNER TO enstore;

--
-- Name: volume_summary2; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW volume_summary2 AS
    SELECT volume.label, volume.capacity_bytes, sum(CASE WHEN (file.size IS NULL) THEN (0)::bigint ELSE file.size END) AS used, count(CASE WHEN (file.deleted = 'n'::bpchar) THEN 1 ELSE NULL::integer END) AS n_active, sum(CASE WHEN ((file.deleted = 'n'::bpchar) AND (file.size IS NOT NULL)) THEN file.size ELSE (0)::bigint END) AS active, count(CASE WHEN (file.deleted = 'y'::bpchar) THEN 1 ELSE NULL::integer END) AS n_deleted, sum(CASE WHEN ((file.deleted = 'y'::bpchar) AND (file.size IS NOT NULL)) THEN file.size ELSE (0)::bigint END) AS deleted, count(CASE WHEN (file.deleted = 'u'::bpchar) THEN 1 ELSE NULL::integer END) AS n_unknown, sum(CASE WHEN ((file.deleted = 'u'::bpchar) AND (file.size IS NOT NULL)) THEN file.size ELSE (0)::bigint END) AS "unknown", volume.remaining_bytes, (((volume.capacity_bytes - volume.remaining_bytes))::numeric - sum(file.size)) AS unaccountable, volume.declared, volume.eod_cookie, volume.first_access, volume.last_access, volume.library, volume.media_type, volume.sum_mounts, volume.sum_rd_access, volume.sum_rd_err, volume.sum_wr_access, volume.sum_wr_err, volume.system_inhibit_0, volume.system_inhibit_1, volume.si_time_0, volume.si_time_1, volume.user_inhibit_0, volume.user_inhibit_1, volume.storage_group, volume.file_family, volume.wrapper, volume."comment" FROM (volume LEFT JOIN file ON ((file.volume = volume.id))) GROUP BY volume.label, volume.capacity_bytes, volume.remaining_bytes, volume.declared, volume.eod_cookie, volume.first_access, volume.last_access, volume.library, volume.media_type, volume.sum_mounts, volume.sum_rd_access, volume.sum_rd_err, volume.sum_wr_access, volume.sum_wr_err, volume.system_inhibit_0, volume.system_inhibit_1, volume.si_time_0, volume.si_time_1, volume.user_inhibit_0, volume.user_inhibit_1, volume.storage_group, volume.file_family, volume.wrapper, volume."comment";


ALTER TABLE public.volume_summary2 OWNER TO enstore;

--
-- Name: volume_with_only_deleted_files; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW volume_with_only_deleted_files AS
    SELECT foo.label, foo.storage_group, foo.file_family, foo.state, foo.media_type, foo.library, foo.n_active, foo.n_deleted, foo.n_unknown FROM (SELECT volume.label, volume.storage_group, volume.file_family, volume.system_inhibit_1 AS state, volume.media_type, volume.library, count(CASE WHEN (file.deleted = 'n'::bpchar) THEN 1 ELSE NULL::integer END) AS n_active, count(CASE WHEN (file.deleted = 'y'::bpchar) THEN 1 ELSE NULL::integer END) AS n_deleted, count(CASE WHEN (file.deleted = 'u'::bpchar) THEN 1 ELSE NULL::integer END) AS n_unknown FROM (volume LEFT JOIN file ON ((file.volume = volume.id))) WHERE (NOT ((volume.label)::text ~~ '%.deleted'::text)) GROUP BY volume.label, volume.storage_group, volume.file_family, volume.system_inhibit_1, volume.media_type, volume.library) foo WHERE ((foo.n_active = 0) AND (foo.n_deleted > 0));


ALTER TABLE public.volume_with_only_deleted_files OWNER TO enstore;

--
-- Name: write_protection_audit; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW write_protection_audit AS
    SELECT volume.id, volume.label, volume.block_size, volume.capacity_bytes, volume.declared, volume.eod_cookie, volume.first_access, volume.last_access, volume.library, volume.media_type, volume.non_del_files, volume.remaining_bytes, volume.sum_mounts, volume.sum_rd_access, volume.sum_rd_err, volume.sum_wr_access, volume.sum_wr_err, volume.system_inhibit_0, volume.system_inhibit_1, volume.si_time_0, volume.si_time_1, volume.user_inhibit_0, volume.user_inhibit_1, volume.storage_group, volume.file_family, volume.wrapper, volume."comment", volume.write_protected FROM volume WHERE ((NOT ((volume.label)::text ~~ '%.deleted'::text)) AND ((((volume.system_inhibit_1)::text <> 'none'::text) AND (volume.write_protected <> 'y'::bpchar)) OR (((volume.system_inhibit_1)::text = 'none'::text) AND (volume.write_protected <> 'n'::bpchar))));


ALTER TABLE public.write_protection_audit OWNER TO enstore;
