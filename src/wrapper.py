# create the dictionary needed by the wrappers
import cern_wrapper

def create_wrapper_dict(ticket):
    # the wrapper section already contains much of the information that we need
    # including all of the information for the cpio_odc_wrapper
    wrapper_d = ticket['wrapper']

    # values needed for the cern wrapper
    ticket_mover = ticket['mover']
    wrapper_d[cern_wrapper.ENCPVERSION] = ticket['version']
    wrapper_d[cern_wrapper.BLOCKLEN] = None                                                   ####
    wrapper_d[cern_wrapper.COMPRESSION] = ticket_mover[cern_wrapper.COMPRESSION]              ####
    wrapper_d[cern_wrapper.MOVERNODE] = ticket_mover[cern_wrapper.MOVERNODE]                  ####
    wrapper_d[cern_wrapper.DRIVEMFG] = ticket_mover[cern_wrapper.DRIVEMFG]                    ####
    wrapper_d[cern_wrapper.DRIVEMODEL] = ticket_mover[cern_wrapper.DRIVEMODEL]                ####
    wrapper_d[cern_wrapper.DRIVESERIALNUMBER] = ticket_mover[cern_wrapper.DRIVESERIALNUMBER]  ####
    wrapper_d[cern_wrapper.CHECKSUM] = None                                                   ####
    
    return wrapper_d
