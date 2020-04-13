from abc import ABC, abstractmethod

__all__ = ['AbstractComputation']


class AbstractComputation(ABC):

    def __init__(self):
        self.__superstep = 0
        self.__messages = {}

    @abstractmethod
    def compute(self, vertex, messages):
        pass

    def pre_superstep(self):
        pass

    def post_superstep(self):
        self.__superstep += 1

    def get_superstep(self):
        return self.__superstep

    def send_message(self, target, message):
        if target not in self.__messages.keys():
            self.__messages[target] = []
        self.__messages[target].append(message)

    def send_message_to_all_edges(self, vertex, message):
        for edge in vertex.get_edges():
            target = edge.get_target_vertex_id()
            self.send_message(target, message)

    def send_message_to_multiple_edges(self, iterable, message):
        for id in iterable:
            self.send_message(id, message)

    def clean_all_messages(self):
        for k in self.__messages.keys():
            self.__messages[k] = []

    def clean_messages(self, id):
        self.__messages[id] = []

    def get_all_messages(self):
        return self.__messages.copy()

    def get_messages(self, key):
        if key in self.__messages.keys():
            res = self.__messages[key]
            self.__messages[key] = []
            return res
        else:
            return []

    def has_messages(self, key):
        if key in self.__messages.keys():
            return self.__messages[key] != []
        else:
            return False


