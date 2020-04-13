class Edge:

    def __init__(self, target, value):
        self.__target = target
        self.__value = value

    def __str__(self):
        return "-> " + str(self.__target) + " w:" + str(self.__value)

    def get_target_vertex_id(self):
        return self.__target

    def get_value(self):
        return self.__value
