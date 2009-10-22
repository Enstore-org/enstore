--
-- Name: status_drive_sn_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_drive_sn_idx ON status USING btree (drive_sn);


--
-- Name: status_host_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_host_idx ON status USING btree (host);


--
-- Name: status_logical_drive_name; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_logical_drive_name ON status USING btree (logical_drive_name);


--
-- Name: status_product_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_product_type_idx ON status USING btree (product_type);


--
-- Name: status_stat_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_stat_type_idx ON status USING btree (stat_type);


--
-- Name: status_tape_volser_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_tape_volser_idx ON status USING btree (tape_volser);


--
-- Name: status_time_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX status_time_idx ON status USING btree ("time");
