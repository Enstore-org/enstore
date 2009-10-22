
--
-- Name: job_definition_name_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX job_definition_name_idx ON job_definition USING btree (name);


--
-- Name: job_finish_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX job_finish_idx ON job USING btree (finish);


--
-- Name: job_name_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX job_name_idx ON job USING btree (name);


--
-- Name: job_start_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX job_start_idx ON job USING btree ("start");


--
-- Name: job_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX job_type_idx ON job USING btree ("type");


--
-- Name: object_job_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX object_job_idx ON "object" USING btree (job);


--
-- Name: object_object_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX object_object_idx ON "object" USING btree ("object");


--
-- Name: progress_job_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX progress_job_idx ON progress USING btree (job);


--
-- Name: progress_start_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX progress_start_idx ON progress USING btree ("start");

