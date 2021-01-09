
from Graph import Graph
import random
import sys
import time

sys.setrecursionlimit(20000)

def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO
    g = Graph()
    for i in range(9000):
        g.insert_vertex( i )

    for node in g.vertices():
        for _ in range(2):
            peso = random.randint( 1, 10000000000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 999 )
            if nodo2 != node.element(): #and  g.peso_unico( peso ):

                e = g.insert_edge( node, g.vertices()[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )
    return g

def creaGrafo():
    g = Graph( False )
    v0 = g.insert_vertex( 0 )
    v1 = g.insert_vertex( 1 )
    v2 = g.insert_vertex( 2 )
    v3 = g.insert_vertex( 3 )
    v4 = g.insert_vertex( 4 )
    v5 = g.insert_vertex( 5 )

    v6 = g.insert_vertex( 6 )
    v7 = g.insert_vertex( 7 )
    v8 = g.insert_vertex( 8 )
    v9 = g.insert_vertex( 9 )
    v10 = g.insert_vertex( 10 )
    v11 = g.insert_vertex( 11 )

    g.insert_edge( v0, v1, 13 )
    g.insert_edge( v0, v2, 6 )
    g.insert_edge( v1, v2, 7 )
    g.insert_edge( v1, v3, 1 )
    g.insert_edge( v2, v3, 14 )
    g.insert_edge( v2, v4, 8 )
    g.insert_edge( v3, v4, 9 )
    g.insert_edge( v3, v5, 3 )
    g.insert_edge( v4, v5, 2 )

    g.insert_edge( v6, v7, 15 )
    g.insert_edge( v6, v8, 5 )
    g.insert_edge( v6, v9, 19 )
    g.insert_edge( v6, v10, 10 )
    g.insert_edge( v7, v9, 17 )
    g.insert_edge( v8, v10, 11 )
    g.insert_edge( v9, v10, 16 )
    g.insert_edge( v9, v11, 4 )
    g.insert_edge( v11, v10, 12 )
    g.insert_edge( v2, v7, 20 )
    g.insert_edge( v4, v9, 18 )
    return g

def findRoot(parent):
    successor_next=[0]*len(parent)
    boolean=True
    while True:
        for i in range(len(parent)):
            successor_next[i]=parent[parent[i]]

        for x,y in zip(successor_next,parent):
            if x!=y:
                print(x,y)
                boolean=False
                break
        if boolean==True:
            break
        for i in range(len(parent)):
            parent[i]=successor_next[i]




def delete_edges(node):
    i = 0
    """
    Questa funzione viene invocara dalle root di ogni componente
    in modo tale da eliminare gli archi avendo come estermità due nodi con lo stesso nome
    cioè che fanno parte della stessa componente
    """
    while i < len( node.listaArchi ):
        edge = node.listaArchi[i]
        n1, n2 = edge.endpoints()
        if n1.element() == n2.element():
            node.listaArchi.pop( i )
        else:
            i = i + 1


def merge(node, root):
    """
    Inserire gli archi del nodo all'interno della lista archi della propria root.
    :param node:
    :param root:
    :return:
    """
    i = 0
    while i < len(node.listaArchi):
        edge = node.listaArchi[i]
        nodo1, nodo2 = edge.endpoints()
        if nodo1.element() != nodo2.element():
            root.listaArchi.append( edge )
            i = i + 1
        else:
            if node == nodo1 or node == nodo2:  # se tra le due estermità non è presente il node in input alla funzione non serve cancellare l'arco dalla sua lista
                node.listaArchi.pop(i)
            else:
                i = i + 1

def Boruvka_seq(g):

    lista_nodi=g.vertices()
    peso_albero=0
    mst=Graph()
    for node in g.vertices():
        mst.insert_vertex(node.element())

    parent=[-1]*g.vertex_count()
    while len(lista_nodi)>1:

        for node in lista_nodi:
            node.isRoot=False
        t1=time.time()
        for node in lista_nodi:
            minedge=None
            for edge in node.listaArchi:

                if minedge==None or minedge.element()>edge.element():
                    minedge=edge
                    n1,n2=edge.endpoints()
                    if n1.element()==node.element():
                        parent[node.element()]=n2.element()
                    else:
                        parent[node.element()]=n1.element()

            if minedge is not None:
                n1,n2=minedge.endpoints()
                e=mst.insert_edge(mst.vertices()[n1.posizione],mst.vertices()[n2.posizione],minedge.element())
                if e is not None:
                    peso_albero+=minedge.element()
                    print(e)
        print(time.time()-t1)

        for node in lista_nodi:
            node_parent=parent[node.element()]
            parent_parent=parent[parent[node.element()]]

            if node.element()==parent_parent:

                if node.element()<node_parent:
                    parent[node.element()]=node.element()

                else:
                    parent[node_parent]=node_parent





        findRoot(parent)

        for j in range( len( parent ) ):
            nodo = g.vertices()[j]
            nodo.root = g.vertices()[parent[nodo.element()]]
            nodo.setElement( nodo.root.element() )  # il nodo prende il nome della root

        i=0
        while i<len(lista_nodi):
            node=lista_nodi[i]
            if node.root!=node:
                merge(node,node.root)
                lista_nodi.pop(i)
            else:
                i=i+1
                delete_edges(node)
    return (mst,peso_albero)

if __name__=='__main__':
    g=creaRandom()
    #g=creaGrafo()

    t=time.time()
    mst,peso=Boruvka_seq(g)
    print("Tempo di esecuzione:",time.time()-t)


    for edge in g.MST_PrimJarnik():
        n1, n2 = edge.endpoints()
        e = mst.get_edge( mst.vertices()[n1.posizione], mst.vertices()[n2.posizione] )
        if e is None:
            print( "ERRORE NELLA COSTRUZIONE DEL MST" )
            break

    if e is not None:
        print( "L'albero costruito è minimo con peso ",peso )

    print( "Numero di archi deve essere n-1 ({}):".format( mst.vertex_count() - 1 ) )
    print( "Bouruvka costruito:", mst.edge_count(), "Prim:", len( g.MST_PrimJarnik() ) )








