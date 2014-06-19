class Study:
        def __init__(self,name=None):
                self.name = name
        # def __del__(self):
        #         print "Iamaway,baby!"
        def __say(self):
                print self.name

        def say(self):
                print self.name

        def _say(self):
                print self.name

        # def __repr__(self):
        #         return "Study('jacky')"
# study = Study("zhuzhengjun")
# study.__say()
# print repr(study)
# print type(eval(repr({})))
# print type(repr(study)) # str
# print type(eval(repr(Study("zhuzhengjun")))) # instance
# study = eval(repr(Study("zhuzhengjun")))
# study.say()