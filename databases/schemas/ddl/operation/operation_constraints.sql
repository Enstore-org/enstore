
--
-- Name: job_definition_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY job_definition
    ADD CONSTRAINT job_definition_pkey PRIMARY KEY (id);


--
-- Name: job_name_key; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_name_key UNIQUE (name);


--
-- Name: job_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: progress_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY progress
    ADD CONSTRAINT progress_pkey PRIMARY KEY (job, task);


--
-- Name: task_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY task
    ADD CONSTRAINT task_pkey PRIMARY KEY (seq, job_type);

--
-- Name: job_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_type_fkey FOREIGN KEY ("type") REFERENCES job_definition(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: object_job_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY "object"
    ADD CONSTRAINT object_job_fkey FOREIGN KEY (job) REFERENCES job(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: progress_job_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY progress
    ADD CONSTRAINT progress_job_fkey FOREIGN KEY (job) REFERENCES job(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: task_job_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY task
    ADD CONSTRAINT task_job_type_fkey FOREIGN KEY (job_type) REFERENCES job_definition(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

