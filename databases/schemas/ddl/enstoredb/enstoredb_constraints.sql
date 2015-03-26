ALTER TABLE ONLY volume
   ADD  CONSTRAINT volume_write_protected_check CHECK ((((write_protected = 'u'::bpchar) OR (write_protected = 'y'::bpchar)) OR (write_protected = 'n'::bpchar)));

--
-- Name: active_file_copying_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY active_file_copying
    ADD CONSTRAINT active_file_copying_pkey PRIMARY KEY (bfid);


--
-- Name: file_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY file
    ADD CONSTRAINT file_pkey PRIMARY KEY (bfid);


--
-- Name: media_capacity_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY media_capacity
    ADD CONSTRAINT media_capacity_pkey PRIMARY KEY ("type");

--
-- Name: migration_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY migration
    ADD CONSTRAINT migration_pkey PRIMARY KEY  (src_bfid,dst_bfid);

--
-- Name: migration_history_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY migration_history
    ADD CONSTRAINT migration_history_pkey PRIMARY KEY (src_vol_id, dst_vol_id);

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
-- Name: sg_count_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY sg_count
    ADD CONSTRAINT sg_count_pkey PRIMARY KEY (library, storage_group);


--
-- Name: state_type_name_key; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY state_type
    ADD CONSTRAINT state_type_name_key UNIQUE (name);


--
-- Name: state_type_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY state_type
    ADD CONSTRAINT state_type_pkey PRIMARY KEY (id);


--
-- Name: volume_label_key; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY volume
    ADD CONSTRAINT volume_label_key UNIQUE (label);


--
-- Name: volume_pkey; Type: CONSTRAINT; Schema: public; Owner: enstore; Tablespace:
--

ALTER TABLE ONLY volume
    ADD CONSTRAINT volume_pkey PRIMARY KEY (id);

--
-- Name: $1; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY file
    ADD CONSTRAINT "$1" FOREIGN KEY (volume) REFERENCES volume(id);


--
-- Name: $1; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY state
    ADD CONSTRAINT "$1" FOREIGN KEY (volume) REFERENCES volume(id) ON DELETE CASCADE;


--
-- Name: $1; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY bad_file
    ADD CONSTRAINT "$1" FOREIGN KEY (bfid) REFERENCES file(bfid) ON DELETE CASCADE;


--
-- Name: $1; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY migration
    ADD CONSTRAINT "$1" FOREIGN KEY (src_bfid) REFERENCES file(bfid) ON DELETE CASCADE;


--
-- Name: $2; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY state
    ADD CONSTRAINT "$2" FOREIGN KEY ("type") REFERENCES state_type(id);


--
-- Name: $2; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY migration
    ADD CONSTRAINT "$2" FOREIGN KEY (dst_bfid) REFERENCES file(bfid) ON DELETE CASCADE;


--
-- Name: file_copies_map_alt_bfid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY file_copies_map
    ADD CONSTRAINT file_copies_map_alt_bfid_fkey FOREIGN KEY (alt_bfid) REFERENCES file(bfid) ON DELETE CASCADE;


--
-- Name: file_copies_map_bfid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY file_copies_map
    ADD CONSTRAINT file_copies_map_bfid_fkey FOREIGN KEY (bfid) REFERENCES file(bfid) ON DELETE CASCADE;


ALTER TABLE ONLY file
    ADD CONSTRAINT file_bfid_key UNIQUE (bfid);

