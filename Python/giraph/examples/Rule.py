class Rule:

    def __init__(self, lhs, rhs, assoc):
        self.__lhs = lhs
        self.__rhs = rhs
        self.__assoc = assoc

    def match(self, e, pos):
        to_match = self.__lhs[pos]
        return to_match == e

    def transform(self, pos):
        return self.__rhs[self.__assoc[pos]]


lhs = {0: 'A', 1: 'B'}
rhs = {0: 'C', 1: 'D'}
assoc = {0: 0, 1: 1}
ab_to_cd = Rule(lhs, rhs, assoc)