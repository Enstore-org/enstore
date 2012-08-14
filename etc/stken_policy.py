# This file contains library manager director policies
policydict = {'CD-LTO4F1.library_manager': {1: {'rule': {'storage_group': 'nova',
                                                    'file_family': 'rawdata_NDOS_unmerged',
                                                    'wrapper': 'cpio_odc',
                                                    },
                                           'minimal_file_size': 5000000000L,
                                           'resulting_library':'CD-DiskSF',
                                           'max_files_in_pack': 50, 
                                           'max_waiting_time': 24*3600,
                                           },
                            },
              'CD-LTO4F1T.library_manager': {2: {'rule': {'storage_group': 'ssa_test',
                                                    'file_family': 'ssa_test',
                                                    'wrapper': 'cpio_odc',
                                                    },
                                           'minimal_file_size': 2000000000L,
                                           'max_files_in_pack': 100, 
                                           'max_waiting_time': 120,
                                           'resulting_library':'CD-DiskSF'
                                           }
                             
                             },
              }
