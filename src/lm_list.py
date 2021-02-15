# library manager list of dictionaries
import os
import Trace
import e_errors


class LMList:
    def __init__(self):
        self.list = []

    # resotre list from DB

    def restore(self):
        pass

    # append to list
    def append(self, element, key=None):
        self.list.append(element)

    # remove from list
    def remove(self, element, key=None):
        self.list.remove(element)
