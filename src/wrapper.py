# create the dictionary needed by the wrappers
import string

import cern_wrapper

MOVER = 'mover'

def create_wrapper_dict(ticket):
    # the wrapper section already contains much of the information that we need
    # including all of the information for the cpio_odc_wrapper
    wrapper_d = ticket['wrapper']
    keys = wrapper_d.keys()
    for key in keys:
        if type(wrapper_d[key]) != type(''):
            wrapper_d[key] = "%s"%(wrapper_d[key],)

    # values needed for the cern wrapper
    ticket_mover = ticket[MOVER]
    wrapper_d[cern_wrapper.ENCPVERSION] = ticket[cern_wrapper.ENCPVERSION]
    wrapper_d[cern_wrapper.BLOCKLEN] = "%s"%(ticket['vc'].get('blocksize', ""),)
    wrapper_d[cern_wrapper.COMPRESSION] = "%s"%(ticket_mover[cern_wrapper.COMPRESSION])
    mnode = ticket_mover[cern_wrapper.MOVERNODE]
    wrapper_d[cern_wrapper.DRIVEMFG] = ticket_mover[cern_wrapper.DRIVEMFG]
    wrapper_d[cern_wrapper.DRIVEMODEL] = ticket_mover[cern_wrapper.DRIVEMODEL]
    wrapper_d[cern_wrapper.DRIVESERIALNUMBER] = ticket_mover[cern_wrapper.DRIVESERIALNUMBER]
    
    return wrapper_d
