
--
-- Name: public; Type: ACL; Schema: -; Owner: products
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM products;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: qa; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE qa FROM PUBLIC;
REVOKE ALL ON TABLE qa FROM enstore;
GRANT ALL ON TABLE qa TO enstore;
GRANT SELECT ON TABLE qa TO enstore_reader;


--
-- Name: tc; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE tc FROM PUBLIC;
REVOKE ALL ON TABLE tc FROM enstore;
GRANT ALL ON TABLE tc TO enstore;
GRANT SELECT ON TABLE tc TO enstore_reader;


--
-- Name: bad_file; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE bad_file FROM PUBLIC;
REVOKE ALL ON TABLE bad_file FROM enstore;
GRANT ALL ON TABLE bad_file TO enstore;
GRANT SELECT ON TABLE bad_file TO enstore_reader;


--
-- Name: volume; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE volume FROM PUBLIC;
REVOKE ALL ON TABLE volume FROM enstore;
GRANT ALL ON TABLE volume TO enstore;
GRANT SELECT ON TABLE volume TO enstore_reader;


--
-- Name: file; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE file FROM PUBLIC;
REVOKE ALL ON TABLE file FROM enstore;
GRANT ALL ON TABLE file TO enstore;
GRANT SELECT ON TABLE file TO enstore_reader;


--
-- Name: file_copies_map; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE file_copies_map FROM PUBLIC;
REVOKE ALL ON TABLE file_copies_map FROM enstore;
GRANT ALL ON TABLE file_copies_map TO enstore;
GRANT SELECT ON TABLE file_copies_map TO enstore_reader;


--
-- Name: enstore_tables; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE enstore_tables FROM PUBLIC;
REVOKE ALL ON TABLE enstore_tables FROM enstore;
GRANT ALL ON TABLE enstore_tables TO enstore;
GRANT SELECT ON TABLE enstore_tables TO enstore_reader;


--
-- Name: file2; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE file2 FROM PUBLIC;
REVOKE ALL ON TABLE file2 FROM enstore;
GRANT ALL ON TABLE file2 TO enstore;
GRANT SELECT ON TABLE file2 TO enstore_reader;


--
-- Name: lock_status; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE lock_status FROM PUBLIC;
REVOKE ALL ON TABLE lock_status FROM enstore;
GRANT ALL ON TABLE lock_status TO enstore;
GRANT SELECT ON TABLE lock_status TO enstore_reader;


--
-- Name: media_capacity; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE media_capacity FROM PUBLIC;
REVOKE ALL ON TABLE media_capacity FROM enstore;
GRANT ALL ON TABLE media_capacity TO enstore;
GRANT SELECT ON TABLE media_capacity TO enstore_reader;


--
-- Name: migration; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE migration FROM PUBLIC;
REVOKE ALL ON TABLE migration FROM enstore;
GRANT ALL ON TABLE migration TO enstore;
GRANT SELECT ON TABLE migration TO enstore_reader;


--
-- Name: migration_history; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE migration_history FROM PUBLIC;
REVOKE ALL ON TABLE migration_history FROM enstore;
GRANT ALL ON TABLE migration_history TO enstore;
GRANT SELECT ON TABLE migration_history TO enstore_reader;


--
-- Name: no_flipping_file_family; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE no_flipping_file_family FROM PUBLIC;
REVOKE ALL ON TABLE no_flipping_file_family FROM enstore;
GRANT ALL ON TABLE no_flipping_file_family TO enstore;
GRANT SELECT ON TABLE no_flipping_file_family TO enstore_reader;


--
-- Name: no_flipping_storage_group; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE no_flipping_storage_group FROM PUBLIC;
REVOKE ALL ON TABLE no_flipping_storage_group FROM enstore;
GRANT ALL ON TABLE no_flipping_storage_group TO enstore;
GRANT SELECT ON TABLE no_flipping_storage_group TO enstore_reader;


--
-- Name: option; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE "option" FROM PUBLIC;
REVOKE ALL ON TABLE "option" FROM enstore;
GRANT ALL ON TABLE "option" TO enstore;
GRANT SELECT ON TABLE "option" TO enstore_reader;


--
-- Name: quota; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE quota FROM PUBLIC;
REVOKE ALL ON TABLE quota FROM enstore;
GRANT ALL ON TABLE quota TO enstore;
GRANT SELECT ON TABLE quota TO enstore_reader;


--
-- Name: remaining_blanks; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE remaining_blanks FROM PUBLIC;
REVOKE ALL ON TABLE remaining_blanks FROM enstore;
GRANT ALL ON TABLE remaining_blanks TO enstore;
GRANT SELECT ON TABLE remaining_blanks TO enstore_reader;


--
-- Name: sg_count; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON sg_count FROM PUBLIC;
REVOKE ALL ON sg_count FROM enstore;
GRANT ALL ON sg_count TO enstore;
GRANT SELECT ON sg_count TO  enstore_reader;

--
-- Name: state; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE state FROM PUBLIC;
REVOKE ALL ON TABLE state FROM enstore;
GRANT ALL ON TABLE state TO enstore;
GRANT SELECT ON TABLE state TO enstore_reader;


--
-- Name: state_type; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE state_type FROM PUBLIC;
REVOKE ALL ON TABLE state_type FROM enstore;
GRANT ALL ON TABLE state_type TO enstore;
GRANT SELECT ON TABLE state_type TO enstore_reader;


--
-- Name: state_type_seq; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON SEQUENCE state_type_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE state_type_seq FROM enstore;
GRANT ALL ON SEQUENCE state_type_seq TO enstore;
GRANT SELECT ON SEQUENCE state_type_seq TO enstore_reader;


--
-- Name: volume_seq; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON SEQUENCE volume_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE volume_seq FROM enstore;
GRANT ALL ON SEQUENCE volume_seq TO enstore;
GRANT SELECT ON SEQUENCE volume_seq TO enstore_reader;


--
-- Name: volume_summary; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE volume_summary FROM PUBLIC;
REVOKE ALL ON TABLE volume_summary FROM enstore;
GRANT ALL ON TABLE volume_summary TO enstore;
GRANT SELECT ON TABLE volume_summary TO enstore_reader;


--
-- Name: volume_summary2; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE volume_summary2 FROM PUBLIC;
REVOKE ALL ON TABLE volume_summary2 FROM enstore;
GRANT ALL ON TABLE volume_summary2 TO enstore;
GRANT SELECT ON TABLE volume_summary2 TO enstore_reader;


--
-- Name: volume_with_only_deleted_files; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE volume_with_only_deleted_files FROM PUBLIC;
REVOKE ALL ON TABLE volume_with_only_deleted_files FROM enstore;
GRANT ALL ON TABLE volume_with_only_deleted_files TO enstore;
GRANT SELECT ON TABLE volume_with_only_deleted_files TO enstore_reader;


--
-- Name: write_protection_audit; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE write_protection_audit FROM PUBLIC;
REVOKE ALL ON TABLE write_protection_audit FROM enstore;
GRANT ALL ON TABLE write_protection_audit TO enstore;
GRANT SELECT ON TABLE write_protection_audit TO enstore_reader;

--
-- Name: files_in_transition; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE files_in_transition FROM PUBLIC;
REVOKE ALL ON TABLE files_in_transition FROM enstore;
GRANT ALL ON TABLE files_in_transition TO enstore;
GRANT SELECT ON TABLE files_in_transition TO enstore_reader;


--
-- Name: cache_statuses; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE cache_statuses FROM PUBLIC;
REVOKE ALL ON TABLE cache_statuses FROM enstore;
GRANT ALL ON TABLE cache_statuses TO enstore;
GRANT SELECT ON TABLE cache_statuses TO enstore_reader;

--
-- Name: archive_statuses; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE archive_statuses FROM PUBLIC;
REVOKE ALL ON TABLE archive_statuses FROM enstore;
GRANT ALL ON TABLE archive_statuses TO enstore;
GRANT SELECT ON TABLE archive_statuses TO enstore_reader;


--
-- PostgreSQL database dump complete
--

