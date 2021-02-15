# this class provides a way to handle dictionaries without having to do a
# dict.has_key or a dict.get for every element.  it supports dictionaries
# contained within dictionaries too.

from UserDict import UserDict


class SafeDict(UserDict):

    def __init__(self, d):
        UserDict.__init__(self)
        for k, v in d.items():
            if isinstance(v, type({})):
                v = SafeDict(v)
            self[k] = v

    def __getitem__(self, key):
        if key in self.data:
            return self.data[key]
        else:
            return SafeDict({})

    def __nonzero__(self):
        return len(self.data.keys())
