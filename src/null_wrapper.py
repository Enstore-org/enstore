###############################################################################
# $Id$


def headers(ticket):
    return '', ''


def hdr_labels(dummy):
    return ""


def eof_labels(dummy):
    return ""


min_header_size = 0


def header_size(header_start):
    return 0


def create_wrapper_dict(ticket):
    return {}


def vol_label_length():
    return 80


def vol_labels(volume_label, ticket={}, own_id=""):
    vol1_label = 'VOL1' + volume_label[0:6]
    return vol1_label + (79 - len(vol1_label)) * ' ' + '0'
