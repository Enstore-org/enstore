class GenericClient ():

    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})
