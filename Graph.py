from Heap import *
import time
class Graph():
    """Representation of a simple graph using an adjacency map."""

    # ------------------------- nested Vertex class -------------------------
    class Vertex():
        """Lightweight vertex structure for a graph."""
        __slots__ = '_element','root','listaArchi','grafo','posizione','parent','isRoot','nodo_riferimento'

        def __init__(self, x,grafo):
            """Do not call constructor directly. Use Graph's insert_vertex(x)."""

            self._element = x
            self.posizione=grafo.vertex_count()
            self.root=self
            self.parent=None
            self.isRoot=False
            self.listaArchi=[]
            self.nodo_riferimento=node_edge_list(self._element,self.posizione)


        def element(self):
            """Return element associated with this vertex."""
            return self._element

        def addArco(self,edge):
            self.listaArchi.append(edge)


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
        __slots__ = '_origin', '_destination', '_element'


        def __init__(self, u, v, x):
            """Do not call constructor directly. Use Graph's insert_edge(u,v,x)."""

            self._origin = u
            self._destination = v
            self._element = x

        def endpoints(self):
            """Return (u,v) tuple for vertices u and v."""
            return (self._origin, self._destination)

        def opposite(self, v):
            """Return the vertex that is opposite v on this edge."""
            if not isinstance( v, Graph.Vertex ):
                raise TypeError( 'v must be a Vertex' )
            return self._destination if v is self._origin else self._origin
            raise ValueError( 'v not incident to edge' )

        def element(self):
            """Return element associated with this edge."""
            return self._element

        def __hash__(self):  # will allow edge to be a map/set key
            return hash( (self._origin, self._destination) )

        def __str__(self):
            return '({0},{1},{2})'.format( self._origin.posizione, self._destination.posizione, self._element )

    # ------------------------- Graph methods -------------------------


    def __init__(self, directed=False):
        """Create an empty graph (undirected, by default).
        Graph is directed if optional paramter is set to True.
        """
        self._outgoing = {}
        # only create second map for directed graph; use alias for undirected
        self._incoming = {} if directed else self._outgoing


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
        total = sum( len( self._outgoing[v] ) for v in self._outgoing )
        # for undirected graphs, make sure not to double-count edges
        return total if self.is_directed() else total // 2

    def edges(self):
        """Return a set of all edges of the graph."""
        result = set()  # avoid double-reporting edges of undirected graph
        for secondary_map in self._outgoing.values():
            result.update( secondary_map.values() )  # add edges to resulting set
        lista=[]
        for x in result:
            lista.append(x)
        return lista

    def get_edge(self, u, v):
        """Return the edge from u to v, or None if not adjacent."""
        self._validate_vertex( u )
        self._validate_vertex( v )
        return self._outgoing[u].get( v )  # returns None if v not adjacent

    def get_edge_by_element(self,n):
        for e in self.edges():
            if e.element()==n:
                return e
        return None

    def get_vertex(self,x):
        for node in self.vertices():
            if node.element()==x:
                return node











    def degree(self, v, outgoing=True):
        """Return number of (outgoing) edges incident to vertex v in the graph.
        If graph is directed, optional parameter used to count incoming edges.
        """
        self._validate_vertex( v )
        adj = self._outgoing if outgoing else self._incoming
        return len( adj[v] )

    def incident_edges(self, v, outgoing=True):
        """Return all (outgoing) edges incident to vertex v in the graph.
        If graph is directed, optional parameter used to request incoming edges.
        """
        lista=[]
        self._validate_vertex( v )
        adj = self._outgoing if outgoing else self._incoming
        for edge in adj[v].values():
            lista.append(edge)
        return lista


    def insert_vertex(self, x=None):
        """Insert and return a new Vertex with element x."""
        v = self.Vertex( x,self )
        self._outgoing[v] = {}
        if self.is_directed():
            self._incoming[v] = {}  # need distinct map for incoming edges
        return v

    def insert_edge(self, u, v, x=None):
        """Insert and return a new Edge from u to v with auxiliary element x.
        Raise a ValueError if u and v are not vertices of the graph.
        Raise a ValueError if u and v are already adjacent.
        """
        if self.get_edge( u, v ) is not None:  # includes error checking
           return None

        e = self.Edge( u.nodo_riferimento, v.nodo_riferimento, x )
        e2=self.Edge( u, v, x )
        self._outgoing[u][v] = e2
        self._incoming[v][u] = e2
        u.addArco(e)
        v.addArco(e)
        return e


    def peso_unico(self,element):
        for edge in self.edges():
            if edge.element()==element:
                return False
        return True

    def BFS(self,nodo,discovered):
        level=[nodo]
        while len(level)>0:
            next_level=[]
            for u in level:
                for e in self.incident_edges(u):
                    v=e.opposite(u)
                    if v not in discovered:
                        discovered[v]=e
                        next_level.append(v)
            level=next_level

    def iscon(self):
        return True
        temp=time.time()
        discovered={self.vertices()[0]:None}
        self.BFS(self.vertices()[0],discovered)
        if len(discovered)==self.vertex_count():
            print(time.time()-temp)
            return True
        return False

    def add_edge_root(self,root,nodo1,nodo2,edge):
        if nodo2.root==root:
            self._outgoing[root][nodo1]=edge
            self._outgoing[nodo1][root]=edge
        elif nodo1.root==root:
            self._outgoing[root][nodo2]=edge
            self._outgoing[nodo2][root]=edge

    def delete_edge(self,node,edge):
        nodo1,nodo2=edge.endpoints()

        if nodo1.posizione==node.element():
            if self.get_edge(node,nodo2) is not None:
                del self._outgoing[node][nodo2]
                del self._incoming[nodo2][node]
        elif nodo2.posizione==node.element():
            if self.get_edge( node, nodo1 ) is not None:
                del self._outgoing[node][nodo1]
                del self._incoming[nodo1][node]



    def MST_PrimJarnik( self ):
        d = {}  # d[v] is bound on distance to tree
        tree = []  # list of edges in spanning tree

        pq = AdaptableHeapPriorityQueue()  # d[v] maps to value (v, e=(u,v))
        pqlocator = {}  # map from vertex to its pq locator
        for v in self.vertices():
            if len( d ) == 0:  # this is the first node
                d[v] = 0  # make it the root
            else:
                d[v] = float( 'inf' )  # positive infinity

            pqlocator[v] = pq.add( d[v], (v, None) )

        while not pq.is_empty():

            key, value = pq.remove_min()
            u, edge = value  # unpack tuple from pq

            del pqlocator[u]  # u is no longer in pq

            if edge is not None:
                tree.append( edge )  # add edge to tree

            for link in self.incident_edges(u):
                v = link.opposite( u )

                if v in pqlocator:  # thus v not yet in tree

                    wgt = link.element()

                    if wgt < d[v]:  # better edge to v?

                     d[v] = wgt  # update the distance

                     pq.update( pqlocator[v], d[v], (v, link) )  # update the pq entry

        return tree



class node_edge_list:
    def __init__(self,element,posizione):
        self.element=element
        self.posizione=posizione
        self.root=None

    def setElement(self,element):
        self.element=element

