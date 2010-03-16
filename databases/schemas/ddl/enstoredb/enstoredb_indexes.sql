
--
-- Name: bad_file_bfid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX bad_file_bfid_idx ON bad_file USING btree (bfid);


--
-- Name: file_copies_map_alt_bfid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_copies_map_alt_bfid_idx ON file_copies_map USING btree (alt_bfid);


--
-- Name: file_copies_map_bfid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_copies_map_bfid_idx ON file_copies_map USING btree (bfid);


--
-- Name: file_crc_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_crc_idx ON file USING btree (crc);


--
-- Name: file_deleted_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_deleted_idx ON file USING btree (deleted);


--
-- Name: file_location_cookie_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_location_cookie_idx ON file USING btree (location_cookie);


--
-- Name: file_pnfs_id_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_pnfs_id_idx ON file USING btree (pnfs_id);


--
-- Name: file_pnfs_path_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_pnfs_path_idx ON file USING btree (pnfs_path);


--
-- Name: file_sanity_crc_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_sanity_crc_idx ON file USING btree (sanity_crc);


--
-- Name: file_sanity_size_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_sanity_size_idx ON file USING btree (sanity_size);


--
-- Name: file_size_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_size_idx ON file USING btree (size);


--
-- Name: file_volume_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX file_volume_idx ON file USING btree (volume);

--
-- Name: migration_dst_bfid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX migration_dst_bfid_idx ON migration USING btree (dst_bfid);

--
-- Name: migration_src_bfid_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX migration_src_bfid_idx ON migration USING btree (src_bfid);

--
-- Name: migration_history_dst_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX migration_history_dst_idx ON migration_history USING btree (dst);


--
-- Name: migration_history_src_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX migration_history_src_idx ON migration_history USING btree (src);


--
-- Name: quota_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX quota_library_idx ON quota USING btree (library);


--
-- Name: quota_storage_group_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX quota_storage_group_idx ON quota USING btree (storage_group);


--
-- Name: state_time_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX state_time_idx ON state USING btree ("time");


--
-- Name: state_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX state_type_idx ON state USING btree ("type");


--
-- Name: state_value_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX state_value_idx ON state USING btree (value);


--
-- Name: state_volume_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX state_volume_idx ON state USING btree (volume);


--
-- Name: volume_declared_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_declared_idx ON volume USING btree (declared);


--
-- Name: volume_eod_cookie_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_eod_cookie_idx ON volume USING btree (eod_cookie);


--
-- Name: volume_file_family_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_file_family_idx ON volume USING btree (file_family);


--
-- Name: volume_label_key_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_label_key_idx ON volume USING btree (label);


--
-- Name: volume_last_access_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_last_access_idx ON volume USING btree (last_access);


--
-- Name: volume_library_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_library_idx ON volume USING btree (library);


--
-- Name: volume_media_type_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_media_type_idx ON volume USING btree (media_type);


--
-- Name: volume_modification_time_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_modification_time_idx ON volume USING btree (modification_time);


--
-- Name: volume_remaining_bytes_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_remaining_bytes_idx ON volume USING btree (remaining_bytes);


--
-- Name: volume_storage_group_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_storage_group_idx ON volume USING btree (storage_group);


--
-- Name: volume_system_inhibit_0_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_system_inhibit_0_idx ON volume USING btree (system_inhibit_0);


--
-- Name: volume_system_inhibit_1_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_system_inhibit_1_idx ON volume USING btree (system_inhibit_1);


--
-- Name: volume_user_inhibit_0_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_user_inhibit_0_idx ON volume USING btree (user_inhibit_0);


--
-- Name: volume_user_inhibit_1_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_user_inhibit_1_idx ON volume USING btree (user_inhibit_1);


--
-- Name: volume_wrapper_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_wrapper_idx ON volume USING btree (wrapper);


--
-- Name: volume_write_protected_idx; Type: INDEX; Schema: public; Owner: enstore; Tablespace:
--

CREATE INDEX volume_write_protected_idx ON volume USING btree (write_protected);

