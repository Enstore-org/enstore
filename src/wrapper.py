# create the dictionary needed by the wrappers
import cern_wrapper

MOVER_WRAPPER_INFO = 'mover_info_wrapper'

def create_wrapper_dict(ticket):
    # the wrapper section already contains much of the information that we need
    # including all of the information for the cpio_odc_wrapper
    wrapper_d = ticket['wrapper']

    # values needed for the cern wrapper
    ticket_mover = ticket[MOVER_WRAPPER_INFO]
    wrapper_d[cern_wrapper.ENCPVERSION] = ticket['version']
    wrapper_d[cern_wrapper.BLOCKLEN] = None                                                   ####
    wrapper_d[cern_wrapper.COMPRESSION] = ticket_mover[cern_wrapper.COMPRESSION]              ####
    wrapper_d[cern_wrapper.MOVERNODE] = ticket_mover[cern_wrapper.MOVERNODE]                  ####
    wrapper_d[cern_wrapper.DRIVEMFG] = ticket_mover[cern_wrapper.DRIVEMFG]                    ####
    wrapper_d[cern_wrapper.DRIVEMODEL] = ticket_mover[cern_wrapper.DRIVEMODEL]                ####
    wrapper_d[cern_wrapper.DRIVESERIALNUMBER] = ticket_mover[cern_wrapper.DRIVESERIALNUMBER]  ####
    
    return wrapper_d
