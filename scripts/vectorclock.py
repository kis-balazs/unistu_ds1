class VectorClock:
    def __init__(self, id=None, copyDict=None):
        assert not (id is not None and copyDict is not None)
        if copyDict is not None:
            self.vcDictionary = copyDict
        else:
            if id == None:
                self.vcDictionary = {}
            else:
                assert copyDict is None
                self.vcDictionary = {str(id): 0}

    def increaseClock(self, id):
        self.vcDictionary[str(id)] += 1

    def addParticipantToClock(self, id):
        self.vcDictionary[str(id)] = 0

    def printClock(self):
        print("\n[VECTORCLOCK]: ", str(self.vcDictionary))
