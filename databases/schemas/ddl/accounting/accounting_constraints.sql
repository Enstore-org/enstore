
--
-- Name: event_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY event
    ADD CONSTRAINT event_pkey PRIMARY KEY (tag);


--
-- Name: mover_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY mover
    ADD CONSTRAINT mover_pkey PRIMARY KEY (name);


--
-- Name: quota_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY quota
    ADD CONSTRAINT quota_pkey PRIMARY KEY (library, storage_group);


--
-- Name: quotas_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY quotas
    ADD CONSTRAINT quotas_pkey PRIMARY KEY ("time", library, storage_group);


--
-- Name: rate_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY rate
    ADD CONSTRAINT rate_pkey PRIMARY KEY ("time");


--
-- Name: reamin_blanks_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY blanks
    ADD CONSTRAINT reamin_blanks_pkey PRIMARY KEY (date, media_type);


--
-- Name: tape_library_slots_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY tape_library_slots_usage
    ADD CONSTRAINT tape_library_slots_usage_pkey PRIMARY KEY ("time", tape_library, "location");


--
-- Name: tape_mounts_tmp_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY tape_mounts_tmp
    ADD CONSTRAINT tape_mounts_tmp_pkey PRIMARY KEY (volume, state);


--
-- Name: write_protect_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY write_protect_summary
    ADD CONSTRAINT write_protect_pkey PRIMARY KEY (date);


--
-- Name: xfer_by_date_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY xfer_by_day
    ADD CONSTRAINT xfer_by_date_pkey PRIMARY KEY (date, storage_group);


--
-- Name: xfer_by_day_by_mover_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY xfer_by_day_by_mover
    ADD CONSTRAINT xfer_by_day_by_mover_pkey PRIMARY KEY (date, mover);


--
-- Name: xfer_by_month_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace: 
--

ALTER TABLE ONLY xfer_by_month
    ADD CONSTRAINT xfer_by_month_pkey PRIMARY KEY (date, storage_group);

--
-- Name: write_protect_summary_by_library_date_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY write_protect_summary_by_library
    ADD CONSTRAINT write_protect_summary_by_library_date_fkey FOREIGN KEY (date) REFERENCES write_protect_summary(date);

