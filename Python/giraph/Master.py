import functools


class Master:

    def __init__(self, vertices, computation):
        self.__graph = vertices
        self.__computation = computation
        self.__run = True

    def __halted(self):
        return functools.reduce(lambda x, y: x and y.is_halted(), self.__graph, True)

    def run(self):
        while self.__run:

            self.__computation.pre_superstep()
            messages = self.__computation.get_all_messages()
            self.__computation.clean_all_messages()

            for vertex in self.__graph:
                if vertex.get_id() in messages.keys():
                    messages_vertex = messages[vertex.get_id()]
                else:
                    messages_vertex = []
                if not vertex.is_halted():
                    self.__computation.compute(vertex, messages_vertex)

            # Activate vertices using messages
            for vertex in self.__graph:
                if self.__computation.has_messages(vertex.get_id()):
                    vertex.wake_up()

            self.__computation.post_superstep()

            self.__run = not self.__halted()

    def __str__(self):
        content = ""
        for v in self.__graph:
            content += str(v) + "\n"
        return content
