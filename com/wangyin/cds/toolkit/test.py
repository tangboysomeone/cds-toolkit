# coding: utf-8
# __author__ = 'Administrator'
# lst = range(2)
# it = iter(lst)
# try:
#     while True:
#         val = it.next()
#         print val
# except StopIteration as ex:
#     print 'error', ex
#
# for idx, ele in enumerate(lst):
#     print idx, ele
#
# itr2  = (x + 1 for x in lst)#生成器表达式
# lst2 =  [x + 1 for x in lst]
# print 'new list', lst2
def list():
    print 'taste my blade'
    return [1, 2, 3, 4]

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

points  = [Point(i, i + 1)
                for i in list()]
for point in points:
    print point.x, point.y



