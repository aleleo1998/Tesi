
from Graph2 import Graph
from random import randint
import sys
from time import time

def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()




    for i in range(10000):
        g.insert_vertex(i)

    nodi=g.vertices()


    for i in range(0,len(nodi)):
        while True:
            peso = randint( 1, 1000000000 )
            if g.peso_unico(peso):
                if i+1==len(nodi):
                    g.insert_edge( nodi[i], nodi[0], peso )
                    break
                else:
                    g.insert_edge( nodi[i], nodi[i+1], peso )
                    break


    for node in nodi:
        i=0
        while i<1399:
            peso = randint( 1, 1000000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0,9999)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):
                i=i+1
                g.insert_edge( node, nodi[nodo2], peso )


    return g





def findRoot(parent):

    successor_next=[0]*len(parent)

    while True:
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




def delete_multi_edges(lista_nodi):
    for node in lista_nodi:
        dict_edge=node.listaArchi.copy()
        for key,edge in dict_edge.items():
            n1,n2=edge.endpoints()
            if n1==n2:
                node.delete_edge(key)

            elif n1==node.element():

                if n2!=key:
                    node.add_arco_root(n2,edge)
                    node.delete_edge(key)
            elif n2==node.element():
                if n1!=key:
                    node.add_arco_root(n1,edge)
                    node.delete_edge(key)


def delete_edges(node):
    i = 0

    edges=node.listaArchi.copy()
    for key,edge in edges.items():
        n1, n2 = edge.endpoints()
        if n1 == n2:
            node.delete_edge(key)
        else:
            i = i + 1


def merge(node, root):
    edges=node.listaArchi.copy()
    for edge in edges.values():
        nodo1, nodo2 = edge.endpoints()
        if nodo1 != nodo2:
            if nodo1==root.element():
                root.add_arco_root(nodo2,edge)
            else:
                root.add_arco_root(nodo1,edge)


def Boruvka_seq(g):
    lista_nodi=g.vertices()
    peso_albero=0
    mst=Graph()
    for node in g.vertices():
        mst.insert_vertex(node.element())

    parent=[-1]*g.vertex_count()

    tempo_ricerca=0
    tempo_pointer=0
    tempo_merge=0

    while len(lista_nodi)>1:



        tempo_ricerca_parziale=time()
        for node in lista_nodi:
            minedge=None
            for edge in node.incident_edges():

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
                    #print(e,flush=True)
        tempo_ricerca+=(time()-tempo_ricerca_parziale)


        for node in lista_nodi:
            node_parent=parent[node.element()]
            parent_parent=parent[parent[node.element()]]

            if node.element()==parent_parent:

                if node.element()<node_parent:
                    parent[node.element()]=node.element()

                else:
                    parent[node_parent]=node_parent




        tempo_pointer_parziale=time()
        findRoot(parent)
        tempo_pointer+=(time()-tempo_pointer_parziale)


        for nodo in lista_nodi:
            nodo.root = g.vertices()[parent[nodo.element()]]
            nodo.setElement( nodo.root.element() )
            for edge in nodo.incident_edges():
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])

        i=0
        tempo_merge_parziale=time()
        while i<len(lista_nodi):
            node=lista_nodi[i]
            if node.root!=node:
                merge(node,node.root)
                lista_nodi.pop(i)
            else:
                i=i+1
        delete_multi_edges(lista_nodi)
        tempo_merge+=(time()-tempo_merge_parziale)
    print("TEMPO RICERCA ARCHI MINIMI :{}, TEMPO POINTER JUMPING:{}, TEMPO MERGE:{}".format(tempo_ricerca,tempo_pointer,tempo_merge))



    return (mst,peso_albero)

if __name__=='__main__':
    g=creaRandom()
    print("archi",g.edges_count)
    t=time()
    mst,peso=Boruvka_seq(g)
    print(mst.edges_count())
    print("Tempo di esecuzione:",time()-t,"COSTO",peso)














