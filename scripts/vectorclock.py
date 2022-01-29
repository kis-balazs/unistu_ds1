class VectorClock:
    def __init__(self, id):
        if id == None:
            self.vcDictionary = {}
        else:
            self.vcDictionary = {(id): 0}

    def increaseClock(self, id):
        self.vcDictionary[(id)] += 1

    def addParticipantToClock(self, id):
        self.vcDictionary[id] = 0

    def printClock(self):
        print("\n[VECTORCLOCK]: ", str(self.vcDictionary))
