#!/usr/bin/env python

import enstore_functions2
import configuration_client
import enstore_show_inventory_cgi

def main():
    # Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
    # if they are not already available.
    enstore_show_inventory_cgi.get_environment()

    #Get the configuration server client.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
   
    #Get information.
    (special, cluster) = enstore_show_inventory_cgi.setup(csc)

    #Set the list (yes, it is really a dictionary) of volumes to empty for
    # the summary.
    catalog = {}

    #
    #Print the html output.
    #
    enstore_show_inventory_cgi.print_html(catalog, special, cluster)
    

if __name__ == "__main__":

    main()
