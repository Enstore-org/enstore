--
-- Name: public; Type: ACL; Schema: -; Owner: products
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM products;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: status; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE status FROM PUBLIC;
REVOKE ALL ON TABLE status FROM enstore;
GRANT ALL ON TABLE status TO enstore;
GRANT SELECT ON TABLE status TO enstore_reader;


--
-- Name: drive_info; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE drive_info FROM PUBLIC;
REVOKE ALL ON TABLE drive_info FROM enstore;
GRANT ALL ON TABLE drive_info TO enstore;
GRANT SELECT ON TABLE drive_info TO enstore_reader;


--
-- PostgreSQL database dump complete
--

