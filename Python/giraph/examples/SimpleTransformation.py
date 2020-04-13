from giraph.BasicComputation import BasicComputation
from giraph.examples.Rule import *


class SimpleTransformation(BasicComputation):

    def compute(self, vertex, messages):
        cur_char, matched_id, matched = vertex.get_value()[0], vertex.get_value()[1], vertex.get_value()[2]

        if self.get_superstep() == 0:
            # match A, and send a request to match B to nghbs
            if ab_to_cd.match(cur_char, 0):
                vertex.set_value((cur_char, 0, matched))
                message = (vertex.get_id(), 1)
                self.send_message_to_all_edges(vertex, message)

        if self.get_superstep() == 1:
            # match B
            for message in messages:
                source, id_element_to_match = message
                if id_element_to_match is None:
                    break
                if ab_to_cd.match(cur_char, id_element_to_match):
                    vertex.set_value((cur_char, id_element_to_match, matched))
                    self.send_message(source, (vertex.get_id(), None))
                    self.send_message(vertex.get_id(), (vertex.get_id(), None))

        if self.get_superstep() == 2:
            # transformation
            new_char = (ab_to_cd.transform(matched_id))
            vertex.set_value((new_char, None, True))

        vertex.vote_to_halt()
