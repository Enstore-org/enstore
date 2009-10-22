--
-- Name: public; Type: ACL; Schema: -; Owner: products
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM products;
GRANT ALL ON SCHEMA public TO products;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: blanks; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE blanks FROM PUBLIC;
REVOKE ALL ON TABLE blanks FROM enstore;
GRANT ALL ON TABLE blanks TO enstore;
GRANT SELECT ON TABLE blanks TO enstore_reader;


--
-- Name: xfer_count_by_day; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE xfer_count_by_day FROM PUBLIC;
REVOKE ALL ON TABLE xfer_count_by_day FROM enstore;
GRANT ALL ON TABLE xfer_count_by_day TO enstore;
GRANT SELECT ON TABLE xfer_count_by_day TO enstore_reader;


--
-- Name: xfer_by_day; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE xfer_by_day FROM PUBLIC;
REVOKE ALL ON TABLE xfer_by_day FROM enstore;
GRANT ALL ON TABLE xfer_by_day TO enstore;
GRANT SELECT ON TABLE xfer_by_day TO enstore_reader;


--
-- Name: xfer_by_day_by_mover; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE xfer_by_day_by_mover FROM PUBLIC;
REVOKE ALL ON TABLE xfer_by_day_by_mover FROM enstore;
GRANT ALL ON TABLE xfer_by_day_by_mover TO enstore;
GRANT SELECT ON TABLE xfer_by_day_by_mover TO enstore_reader;


--
-- Name: drive_utilization; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE drive_utilization FROM PUBLIC;
REVOKE ALL ON TABLE drive_utilization FROM enstore;
GRANT ALL ON TABLE drive_utilization TO enstore;
GRANT SELECT ON TABLE drive_utilization TO enstore_reader;


--
-- Name: encp_error; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE encp_error FROM PUBLIC;
REVOKE ALL ON TABLE encp_error FROM enstore;
GRANT ALL ON TABLE encp_error TO enstore;
GRANT SELECT ON TABLE encp_error TO enstore_reader;


--
-- Name: encp_xfer; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE encp_xfer FROM PUBLIC;
REVOKE ALL ON TABLE encp_xfer FROM enstore;
GRANT ALL ON TABLE encp_xfer TO enstore;
GRANT SELECT ON TABLE encp_xfer TO enstore_reader;


--
-- Name: encp_xfer_average_by_storage_group; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE encp_xfer_average_by_storage_group FROM PUBLIC;
REVOKE ALL ON TABLE encp_xfer_average_by_storage_group FROM enstore;
GRANT ALL ON TABLE encp_xfer_average_by_storage_group TO enstore;
GRANT SELECT ON TABLE encp_xfer_average_by_storage_group TO enstore_reader;


--
-- Name: enstore_tables; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE enstore_tables FROM PUBLIC;
REVOKE ALL ON TABLE enstore_tables FROM enstore;
GRANT ALL ON TABLE enstore_tables TO enstore;
GRANT SELECT ON TABLE enstore_tables TO enstore_reader;


--
-- Name: event; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE event FROM PUBLIC;
REVOKE ALL ON TABLE event FROM enstore;
GRANT ALL ON TABLE event TO enstore;
GRANT SELECT ON TABLE event TO enstore_reader;


--
-- Name: lock_status; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE lock_status FROM PUBLIC;
REVOKE ALL ON TABLE lock_status FROM enstore;
GRANT ALL ON TABLE lock_status TO enstore;
GRANT SELECT ON TABLE lock_status TO enstore_reader;


--
-- Name: mover; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE mover FROM PUBLIC;
REVOKE ALL ON TABLE mover FROM enstore;
GRANT ALL ON TABLE mover TO enstore;
GRANT SELECT ON TABLE mover TO enstore_reader;


--
-- Name: old_bytes_per_day; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE old_bytes_per_day FROM PUBLIC;
REVOKE ALL ON TABLE old_bytes_per_day FROM enstore;
GRANT ALL ON TABLE old_bytes_per_day TO enstore;
GRANT SELECT ON TABLE old_bytes_per_day TO enstore_reader;


--
-- Name: quota; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE quota FROM PUBLIC;
REVOKE ALL ON TABLE quota FROM enstore;
GRANT ALL ON TABLE quota TO enstore;
GRANT SELECT ON TABLE quota TO enstore_reader;


--
-- Name: tape_mounts; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE tape_mounts FROM PUBLIC;
REVOKE ALL ON TABLE tape_mounts FROM enstore;
GRANT ALL ON TABLE tape_mounts TO enstore;
GRANT SELECT ON TABLE tape_mounts TO enstore_reader;


--
-- Name: tape_mounts_tmp; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE tape_mounts_tmp FROM PUBLIC;
REVOKE ALL ON TABLE tape_mounts_tmp FROM enstore;
GRANT ALL ON TABLE tape_mounts_tmp TO enstore;
GRANT SELECT ON TABLE tape_mounts_tmp TO enstore_reader;


--
-- Name: tmp_xfer_by_day; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE tmp_xfer_by_day FROM PUBLIC;
REVOKE ALL ON TABLE tmp_xfer_by_day FROM enstore;
GRANT ALL ON TABLE tmp_xfer_by_day TO enstore;
GRANT SELECT ON TABLE tmp_xfer_by_day TO enstore_reader;


--
-- Name: write_protect_summary; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE write_protect_summary FROM PUBLIC;
REVOKE ALL ON TABLE write_protect_summary FROM enstore;
GRANT ALL ON TABLE write_protect_summary TO enstore;
GRANT SELECT ON TABLE write_protect_summary TO enstore_reader;


--
-- Name: write_protect_summary_by_library; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE write_protect_summary_by_library FROM PUBLIC;
REVOKE ALL ON TABLE write_protect_summary_by_library FROM enstore;
GRANT ALL ON TABLE write_protect_summary_by_library TO enstore;
GRANT SELECT ON TABLE write_protect_summary_by_library TO enstore_reader;


--
-- Name: xfer_by_month; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE xfer_by_month FROM PUBLIC;
REVOKE ALL ON TABLE xfer_by_month FROM enstore;
GRANT ALL ON TABLE xfer_by_month TO enstore;
GRANT SELECT ON TABLE xfer_by_month TO enstore_reader;


--
-- PostgreSQL database dump complete
--

