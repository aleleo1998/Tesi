
from Graph import Graph
import random
import sys
import time

sys.setrecursionlimit(20000)

def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO
    g = Graph()
    for i in range(10000):
        g.insert_vertex( i )
    nodi=g.vertices()
    for node in g.vertices():
        for _ in range(2):
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 9999)
            if nodo2 != node.element(): #and  g.peso_unico( peso ):

                e = g.insert_edge( node,nodi[nodo2], peso )
                if e is None:
                    pass#print( "non inserisco" )
                else:
                    pass#print( "Inserisco" )
            else:
               pass# print( "non inserisco" )
    return g



def findRoot(parent):

    successor_next=[0]*len(parent)

    while True:
        print("ITERAZIONE POINTER JUMPING",flush=True)
        boolean=True
        for i in range(len(parent)):
            successor_next[i]=parent[parent[i]]

        for x,y in zip(successor_next,parent):
            if x!=y:
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
        if n1 == n2:

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
        if nodo1 != nodo2:

            root.listaArchi.append( edge )
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

        for node in lista_nodi:
            minedge=None
            for edge in node.listaArchi:

                if minedge==None or minedge.element()>edge.element():
                    minedge=edge
                    n1,n2=edge.endpoints()
                    if n1==node.element():
                        parent[node.element()]=n2
                    else:
                        parent[node.element()]=n1

            if minedge is not None:
                n1,n2=minedge.endpoints_posizione()
                e=mst.insert_edge(mst.vertices()[n1],mst.vertices()[n2],minedge.element())
                if e is not None:
                    peso_albero+=minedge.element()
                    print(e,flush=True)


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
            nodo.setElement( nodo.root.element() )
            for edge in nodo.listaArchi:
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])

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








