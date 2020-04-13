class Vertex:

    def __init__(self, id, value):
        self.__id = id
        self.__active = True
        self.__value = value
        self.__edges = []

    def __str__(self):
        content = "id: " + str(self.__id) + "\tval: " + str(self.__value) + "\n"
        for e in self.__edges:
            content += "  " + str(e)
        return content

    def vote_to_halt(self):
        self.__active = False

    def wake_up(self):
        self.__active = True

    def get_id(self):
        return self.__id

    def set_value(self, val):
        self.__value = val

    def get_value(self):
        return self.__value

    def add_edge(self, edge):
        self.__edges.append(edge)

    def get_edges(self):
        return self.__edges

    def get_all_edge_values(self):
        res = []
        for edge in self.__edges:
            res.append(edge.get_value())

    def get_num_edges(self):
        return len(self.__edges)

    def is_halted(self):
        return not self.__active
