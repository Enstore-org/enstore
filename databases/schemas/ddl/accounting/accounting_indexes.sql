
CREATE INDEX blanks_date_idx ON blanks USING btree (date);


--
-- Name: blanks_media_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX blanks_media_type_idx ON blanks USING btree (media_type);


--
-- Name: drive_utilization_tape_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX drive_utilization_tape_library_idx ON drive_utilization USING btree (tape_library);


--
-- Name: drive_utilization_time_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX drive_utilization_time_idx ON drive_utilization USING btree ("time");


--
-- Name: drive_utilization_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX drive_utilization_type_idx ON drive_utilization USING btree ("type");


--
-- Name: encp_error_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_error_library_idx ON encp_xfer USING btree (library);


--
-- Name: encp_xfer_average_by_storage_group_date_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_date_index ON encp_xfer_average_by_storage_group USING btree (date);


--
-- Name: encp_xfer_average_by_storage_group_from_date_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_from_date_index ON encp_xfer_average_by_storage_group USING btree (from_date);


--
-- Name: encp_xfer_average_by_storage_group_rw_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_rw_index ON encp_xfer_average_by_storage_group USING btree (rw);


--
-- Name: encp_xfer_average_by_storage_group_storage_group_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_storage_group_index ON encp_xfer_average_by_storage_group USING btree (storage_group);


--
-- Name: encp_xfer_average_by_storage_group_to_date_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_to_date_index ON encp_xfer_average_by_storage_group USING btree (to_date);


--
-- Name: encp_xfer_average_by_storage_group_unix_time_index; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_average_by_storage_group_unix_time_index ON encp_xfer_average_by_storage_group USING btree (unix_time);


--
-- Name: encp_xfer_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX encp_xfer_library_idx ON encp_xfer USING btree (library);


--
-- Name: error_date_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_date_idx ON encp_error USING btree (date);


--
-- Name: error_file_family_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_file_family_idx ON encp_error USING btree (file_family);


--
-- Name: error_oid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_oid_idx ON encp_error USING btree (oid);


--
-- Name: error_storage_group_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_storage_group_idx ON encp_error USING btree (storage_group);


--
-- Name: error_volume_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_volume_idx ON encp_error USING btree (volume);


--
-- Name: error_wrapper_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX error_wrapper_idx ON encp_error USING btree (wrapper);


--
-- Name: event_name_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX event_name_idx ON event USING btree (name);


--
-- Name: event_node_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX event_node_idx ON event USING btree (node);


--
-- Name: event_oid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX event_oid_idx ON event USING btree (oid);


--
-- Name: event_start_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX event_start_idx ON event USING btree ("start");


--
-- Name: mover_logname_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX mover_logname_idx ON mover USING btree (logname);


--
-- Name: mover_media_changer_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX mover_media_changer_idx ON mover USING btree (media_changer);


--
-- Name: quota_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX quota_library_idx ON quota USING btree (library);


--
-- Name: quota_storage_group_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX quota_storage_group_idx ON quota USING btree (storage_group);


--
-- Name: tape_library_slots_usage_location_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_library_slots_usage_location_idx ON tape_library_slots_usage USING btree ("location");


--
-- Name: tape_library_slots_usage_media_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_library_slots_usage_media_type_idx ON tape_library_slots_usage USING btree (media_type);


--
-- Name: tape_library_slots_usage_tape_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_library_slots_usage_tape_library_idx ON tape_library_slots_usage USING btree (tape_library);


--
-- Name: tape_library_slots_usage_time_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_library_slots_usage_time_idx ON tape_library_slots_usage USING btree ("time");


--
-- Name: tape_mnts_node_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mnts_node_idx ON tape_mounts USING btree (node);


--
-- Name: tape_mnts_oid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mnts_oid_idx ON tape_mounts USING btree (oid);


--
-- Name: tape_mnts_start_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mnts_start_idx ON tape_mounts USING btree ("start");


--
-- Name: tape_mnts_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mnts_type_idx ON tape_mounts USING btree ("type");


--
-- Name: tape_mnts_volume_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mnts_volume_idx ON tape_mounts USING btree (volume);


--
-- Name: tape_mounts_logname_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tape_mounts_logname_idx ON tape_mounts USING btree (logname);


--
-- Name: tmt_oid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX tmt_oid_idx ON tape_mounts_tmp USING btree (oid);


--
-- Name: write_protect_summary_by_library_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX write_protect_summary_by_library_library_idx ON write_protect_summary_by_library USING btree (library);


--
-- Name: xfr_date_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_date_idx ON encp_xfer USING btree (date);


--
-- Name: xfr_file_family_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_file_family_idx ON encp_xfer USING btree (file_family);


--
-- Name: xfr_media_changer_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_media_changer_idx ON encp_xfer USING btree (media_changer);


--
-- Name: xfr_mover_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_mover_idx ON encp_xfer USING btree (mover);


--
-- Name: xfr_node_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_node_idx ON encp_xfer USING btree (node);


--
-- Name: xfr_oid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_oid_idx ON encp_xfer USING btree (oid);


--
-- Name: xfr_pid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_pid_idx ON encp_xfer USING btree (pid);


--
-- Name: xfr_storage_group_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_storage_group_idx ON encp_xfer USING btree (storage_group);


--
-- Name: xfr_user_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_user_idx ON encp_xfer USING btree (username);


--
-- Name: xfr_volume_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_volume_idx ON encp_xfer USING btree (volume);


--
-- Name: xfr_wrapper_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace: 
--

CREATE INDEX xfr_wrapper_idx ON encp_xfer USING btree (wrapper);



