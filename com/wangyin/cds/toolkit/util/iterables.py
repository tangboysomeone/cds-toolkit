__author__ = 'zy'


def find(itr, predicate):
    for element in itr:
        if predicate(element):
            return element
