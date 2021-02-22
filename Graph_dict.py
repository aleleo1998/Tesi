from multiprocessing import Manager

class Graph():
    """Representation of a simple graph using an adjacency map."""

    # ------------------------- nested Vertex class -------------------------
    class Vertex():
        """Lightweight vertex structure for a graph."""
        __slots__ = '_element','root','listaArchi','posizione','archi_condivisi'
        #_fields_=[('_element',c_int),('root',c_int),('listaArchi',Graph.Edge),('posizione',c_int)]
        def __init__(self, x):
            """Do not call constructor directly. Use Graph's insert_vertex(x)."""

            self._element = x
            self.posizione=x
            self.root=None
            self.listaArchi={}
            self.archi_condivisi=None




        def element(self):
            """Return element associated with this vertex."""
            return self._element


        def addEdge(self,e):
            opposite=e.opposite(self.element())
            self.listaArchi[opposite]=e
            self.archi_condivisi[opposite]=e.element()

        def incident_edges(self):
            for edge in self.listaArchi.values():
                yield edge

        def add_arco_root(self,node,edge):
            e=self.listaArchi.get(node)
            if e is None:
                self.listaArchi[node]=edge
                self.archi_condivisi[node]=edge.element()
            else:
                if e.element()>edge.element():
                    self.listaArchi[node]=edge
                    self.archi_condivisi[node]=e.element()







        def delete_edge(self,i):
            self.listaArchi.pop(i)
            self.archi_condivisi.pop(i)







        def setParent(self,x):
            self.parent=x

        def setElement(self,x):
            self._element=x



        def __hash__(self):  # will allow vertex to be a map/set key
            return hash( id( self ) )

        def __str__(self):
            return str( self.element() )

    # ------------------------- nested Edge class -------------------------
    class Edge():



        #_fields_=[('_origin',c_int),('_destination',c_int),('posizione1',c_int),('posizione2',c_int)]
        def __init__(self, u, v, x):
            #Do not call constructor directly. Use Graph's insert_edge(u,v,x).

            self._origin = u
            self._destination = v
            self.posizione1=u
            self.posizione2=v

            self._element = x



        def endpoints(self):
            #Return (u,v) tuple for vertices u and v.
            return (self._origin, self._destination)

        def endpoints_posizione(self):
            return(self.posizione1,self.posizione2)

        def setElement(self,el1,el2):
            self._origin=el1
            self._destination=el2

        def opposite(self, v):
            #Return the vertex that is opposite v on this edge.
            return self._destination if v is self._origin else self._origin
            raise ValueError( 'v not incident to edge' )

        def element(self):
            #Return element associated with this edge.
            return self._element

        def __hash__(self):  # will allow edge to be a map/set key
            return hash( (self._origin, self._destination) )

        def __str__(self):
            return '({0},{1},{2})'.format( self._origin, self._destination, self._element )





    def __init__(self):

        self.pesi={}
        self.nodes=[]
        self.archi=[]


    def _validate_vertex(self, v):
        """Verify that v is a Vertex of this graph."""
        if not isinstance( v, self.Vertex ):
            raise TypeError( 'Vertex expected' )
        if v not in self.nodes:
            raise ValueError( 'Vertex does not belong to this graph.' )

    def vertex_count(self):
        """Return the number of vertices in the graph."""
        return len( self.nodes )

    def vertices(self):
        """Return an iteration of all vertices of the graph."""
        lista=[]
        for x in self.nodes:
            lista.append(x)
        return lista

    def edges_count(self):
        """Return the number of edges in the graph."""
        total = sum( len(node.listaArchi) for node in self.nodes )
        # for undirected graphs, make sure not to double-count edges
        return  total // 2

    def edges(self):
        """Return a set of all edges of the graph."""
        lista=set()
        for node in self.nodes:
            for edge in node.incident_edges():
                lista.add(edge)
        return lista

    def get_edge(self, u, v):
        return u.listaArchi.get(v.element())





    def insert_vertex(self, x=None):
        """Insert and return a new Vertex with element x."""
        v = self.Vertex(x)
        self.nodes.append(v)
        return v

    def insert_edge(self, u, v, x=None):
        if self.get_edge( u, v ) is not None:  # includes error checking
            return None

        e=self.Edge(u.element(),v.element(),x)
        u.addEdge(e)
        v.addEdge(e)
        self.archi.append(e)
        self.pesi[x]=True
        return e

    def peso_unico(self,x):
        peso=self.pesi.get(x)
        if peso is None:
            return True
        return False








