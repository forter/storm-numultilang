import storm

class SplitSentenceBolt(storm.BasicBolt):
    words=''

    def initialize(self, stormconf, context):
        super(SplitSentenceBolt, self).initialize(stormconf, context)
        with open('readfile.txt','r') as f:
            self.words = f.read()
        pass

    def process(self, tup):
        storm.emit([self.words])

SplitSentenceBolt().run()


