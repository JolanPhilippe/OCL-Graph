from giraph.Vertex import *
from giraph.Edge import *
from giraph.examples.SimpleTransformation import *
from giraph.Master import *

# import random
# import string

if __name__ == "__main__":
	v1 = Vertex(1, ('A', None, False))
	v2 = Vertex(2, ('B', None, False))
	v3 = Vertex(3, ('C', None, False))
	v4 = Vertex(4, ('B', None, False))
	v5 = Vertex(5, ('A', None, False))
	v6 = Vertex(6, ('B', None, False))
	v1.add_edge(Edge(2, None))
	v1.add_edge(Edge(3, None))
	v2.add_edge(Edge(3, None))
	v3.add_edge(Edge(4, None))
	v5.add_edge(Edge(6, None))
	vertices = [v1, v2, v3, v4, v5, v6]
	st = SimpleTransformation()

	m = Master(vertices, st)
	print(m)

	res = m.run()
	# for s in vertices:
	print(m)