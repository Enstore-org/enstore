class ClientDefaults:

    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})

    def start_backup(self):
    	return self.send({'work':'start_backup'})

    def stop_backup(self):
    	return self.send({'work':'stop_backup'})	

