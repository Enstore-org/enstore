
DictionaryLockedError = "DictionaryLockedError"

            
class LockingDict:

    def __init__(self):
        self.dict = {}
        self.locked = 0

    def lock(self):
        self.locked = 1

    def unlock(self):
        self.unlocked = 0

    def __getitem__(self,key):
        return self.dict[key]

    def __setitem__(self,key,value):
        if self.locked:
            raise DictionaryLockedError, (key,value)

        self.dict[key]=value

    def __repr__(self):
        return repr(self.dict)

    
        
