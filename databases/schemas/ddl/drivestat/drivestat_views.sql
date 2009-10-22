
--
-- Name: drive_info; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW drive_info AS
    SELECT DISTINCT ON (status.logical_drive_name) status.logical_drive_name AS drive, status.host, status.product_type AS "type", status.drive_vendor AS vendor, status.drive_sn AS sn, status.firmware_version AS firmware FROM status ORDER BY status.logical_drive_name, status."time" DESC;


ALTER TABLE public.drive_info OWNER TO enstore;

