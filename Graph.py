from Heap import *
import time
class Graph():
    """Representation of a simple graph using an adjacency map."""

    # ------------------------- nested Vertex class -------------------------
    class Vertex():
        """Lightweight vertex structure for a graph."""
        __slots__ = '_element','root','listaArchi','posizione','parent','isRoot'
        def __init__(self, x,g):
            """Do not call constructor directly. Use Graph's insert_vertex(x)."""

            self._element = x
            self.posizione=x
            self.root=None
            self.parent=None
            self.isRoot=False
            self.listaArchi=[]




        def element(self):
            """Return element associated with this vertex."""
            return self._element


        def addArco(self,e):
            self.listaArchi.append(e)





        def delete_edge(self,i):
            self.listaArchi.pop(i)







        def setParent(self,x):
            self.parent=x

        def setElement(self,x):
            self._element=x



        def __hash__(self):  # will allow vertex to be a map/set key
            return hash( id( self ) )

        def __str__(self):
            return str( self.element() )

    # ------------------------- nested Edge class -------------------------
    class Edge:
        """Lightweight edge structure for a graph."""
        __slots__ = '_origin', '_destination', '_element','posizione1','posizione2'


        def __init__(self, u, v, x):
            """Do not call constructor directly. Use Graph's insert_edge(u,v,x)."""

            self._origin = u
            self._destination = v
            self.posizione1=u
            self.posizione2=v

            self._element = x

        def endpoints(self):
            """Return (u,v) tuple for vertices u and v."""
            return (self._origin, self._destination)

        def endpoints_posizione(self):
            return(self.posizione1,self.posizione2)

        def setElement(self,el1,el2):
            self._origin=el1
            self._destination=el2

        def opposite(self, v):
            """Return the vertex that is opposite v on this edge."""
            return self._destination if v is self._origin else self._origin
            raise ValueError( 'v not incident to edge' )

        def element(self):
            """Return element associated with this edge."""
            return self._element

        def __hash__(self):  # will allow edge to be a map/set key
            return hash( (self._origin, self._destination) )

        def __str__(self):
            return '({0},{1},{2})'.format( self._origin, self._destination, self._element )

    # ------------------------- Graph methods -------------------------


    def __init__(self, directed=False):
        """Create an empty graph (undirected, by default).
        Graph is directed if optional paramter is set to True.
        """
        self._outgoing = {}
        # only create second map for directed graph; use alias for undirected
        self._incoming = {} if directed else self._outgoing
        self.pesi={}


    def _validate_vertex(self, v):
        """Verify that v is a Vertex of this graph."""
        if not isinstance( v, self.Vertex ):
            raise TypeError( 'Vertex expected' )
        if v not in self._outgoing:
            raise ValueError( 'Vertex does not belong to this graph.' )

    def is_directed(self):
        """Return True if this is a directed graph; False if undirected.
        Property is based on the original declaration of the graph, not its contents.
        """
        return self._incoming is not self._outgoing  # directed if maps are distinct

    def vertex_count(self):
        """Return the number of vertices in the graph."""
        return len( self._outgoing )

    def vertices(self):
        """Return an iteration of all vertices of the graph."""
        lista=[]
        for x in self._outgoing.keys():
            lista.append(x)
        return lista

    def edge_count(self):
        """Return the number of edges in the graph."""
        total = sum( len(node.listaArchi) for node in self.vertices() )
        # for undirected graphs, make sure not to double-count edges
        return total if self.is_directed() else total // 2

    def edges(self):
        """Return a set of all edges of the graph."""
        lista=set()
        for node in self.vertices():
            for edge in node.listaArchi:
                lista.add(edge)
        return lista

    def get_edge(self, u, v):
         for edge in u.listaArchi:
             n1,n2=edge.endpoints_posizione()
             if (n1==u.posizione and n2==v.posizione) or (n2==u.posizione and n1==v.posizione):
                 return edge




    def insert_vertex(self, x=None):
        """Insert and return a new Vertex with element x."""
        v = self.Vertex(x,self)
        self._outgoing[v] = {}
        if self.is_directed():
            self._incoming[v] = {}  # need distinct map for incoming edges
        return v

    def insert_edge(self, u, v, x=None):
        if self.get_edge( u, v ) is not None:  # includes error checking
           return None

        e=self.Edge(u.element(),v.element(),x)
        u.addArco(e)
        v.addArco(e)
        self.pesi[x]=True
        return e

    def peso_unico(self,x):
        try:
            peso=self.pesi[x]
            return False
        except:
            return True










