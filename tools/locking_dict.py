
DictionaryLockedError = "DictionaryLockedError"

            
class LockingDict:

    def __init__(self):
        self.dict = {}
        self._locked = 0

    def lock(self):
        self._locked = 1

    def unlock(self):
        self._locked = 0

    def locked(self):
        return self._locked
        
    def __getitem__(self,key):
        return self.dict[key]

    def __setitem__(self,key,value):
        if self._locked:
            raise DictionaryLockedError, (key,value)

        self.dict[key]=value

    def __len__(self):
        return  len(self.dict)
    
    def __getattr__(self,attr):
        return getattr(self.dict,attr)

    
        
