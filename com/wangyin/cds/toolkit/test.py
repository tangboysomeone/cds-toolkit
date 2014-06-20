list = [1, 2, 3]


class Iterables:
    @classmethod
    def find(cls, itr, predicate):
        for element in itr:
            if predicate(element):
                return element


print Iterables.find(list, lambda s: s == 2)