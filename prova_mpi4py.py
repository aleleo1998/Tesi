from Graph import *
from mpi4py import MPI
import random
import sys
from multiprocessing import Array
import time

sys.setrecursionlimit(20000)
def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    for i in range(5000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        i=0
        while i<2:
            peso = random.randint( 1, 10000000000000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 999 )
            if nodo2 != node.element(): #and g.get_edge(node,nodi[nodo2]) is None: #and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is not None:
                    #print("inserisco")
                    i+=1

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


def dividi_gruppi(lista_nodi, n):
    i = 0

    num = int( len( lista_nodi ) / n ) + 1
    cont = 0
    lista_return = [[] for _ in range( n )]  # lista di ritorno

    while i < len( lista_nodi ):
        for _ in range( int( num ) ):
            if cont == n:
                """
                il contatore cont riparte da zero in modo tale da avere una distribuzione uniforme
                dei nodi sulle varie liste
                """
                cont = 0
            lista_return[cont].append(lista_nodi[i])
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return lista_return

if __name__=="__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size=comm.Get_size()


    if rank == 0:

            #g=creaGrafo()
            g=creaRandom()
            parent=[0]*g.vertex_count()
            successor_next=[-1]*len(parent)

            if g.iscon():
                grafoB=Graph()
                for i in range(g.vertex_count()):
                    grafoB.insert_vertex(i)
                lista_nodi_grafoB=grafoB.vertices()
                lista_nodi_originale=g.vertices()
                lista_nodi_rimanenti=g.vertices()

                t77=time.time()
                for i in range(0,6) :

                    t=time.time()
                    minimiArchi=[]

                    if i==0:
                        for i in range(1,size):
                            lista_divisa=dividi_gruppi(lista_nodi_rimanenti,size-1)
                            comm.send(lista_divisa[i-1],dest=i)




                    #t1=time.time()
                    for i in range(1,size):
                        minimiArchi.append(comm.recv(source=i))

                    for r in minimiArchi:
                        for edge,node in r:
                            n1,n2=edge.endpoints()
                            e=grafoB.insert_edge(lista_nodi_grafoB[n1.posizione]
                                               ,lista_nodi_grafoB[n2.posizione]
                                               ,edge.element())
                            if n1.element()==node:
                                parent[node]=n2.element()
                            elif n2.element()==node:
                                parent[node]=n1.element()

                            if e is not None:
                                print("Inserisco",e)


                    for i in range(len(parent)):
                        opposto=parent[parent[i]]

                        if i==opposto:

                            if i<parent[i]:
                                parent[i]=i
                            else:
                                parent[parent[i]]=parent[i]

                    while True:
                        for i in range(1,size):
                            comm.send(parent,dest=i)


                        for i in range(1,size):

                            successor=comm.recv(source=i)
                            for i in range(len(successor)):
                                if successor[i]!=-1:
                                    successor_next[i]=successor[i]

                        for x,y in zip(parent,successor_next):
                            if x!=y:
                                pass #print(x,y)

                        if successor_next==parent:
                            for i in range(1,size):
                                comm.send(False,dest=i)
                            break



                        for i in range(len(parent)):
                            parent[i]=successor_next[i]
                    #print("tempo find root",time.time()-t1)


                    for j in range( len( parent ) ):
                        nodo = lista_nodi_originale[j]
                        nodo.root = lista_nodi_originale[parent[nodo.element()]]
                        nodo.setElement( nodo.root.element() )# il nodo prende il nome della root


                    for i in range(1,size):
                        comm.send(lista_nodi_rimanenti,dest=i)



                    lista_nodi_rimanenti=[]
                    for i in range(1,size):
                        lista_return=comm.recv(source=i)
                        for node in lista_return:
                            n=lista_nodi_originale[node.element()]
                            if n not in lista_nodi_rimanenti:


                                n.listaArchi=[]
                                for edge in node.listaArchi:
                                    n1,n2=edge.endpoints()
                                    e=Graph.Edge(lista_nodi_originale[n1.posizione],lista_nodi_originale[n2.posizione],edge.element())
                                    n.listaArchi.append(e)
                                lista_nodi_rimanenti.append(n)
                        #if len(lista_nodi_rimanenti)<=1:
                            #break




                    #print('tempo merge',time.time()-t)

                print(grafoB.iscon())
                print("tempo",time.time()-t77)


                peso_prim=0
                cont=0
                for edge in g.MST_PrimJarnik():
                    n1, n2 = edge.endpoints()
                    peso_prim+=edge.element()
                    e = grafoB.get_edge( grafoB.vertices()[n1.posizione], grafoB.vertices()[n2.posizione] )
                    if e is None:
                        print( "ERRORE NELLA COSTRUZIONE DEL MST" )
                        break


                if e is not None:
                    print( "L'albero costruito è minimo con peso",peso_prim )




        #if e is not None:
                   # print( "L'albero costruito è minimo con peso",peso_prim )

    elif rank!=0:

        for i in range(0,6):

                #if lista_nodi==False:
                    #break

                if i==0:
                    lista_nodi=comm.recv(source=0)
                risultati=[]


                for node in lista_nodi:
                    minEdge=None
                    for edge in node.listaArchi:
                        if minEdge is None or minEdge.element()>edge.element():
                            minEdge=edge


                    if minEdge is not None:
                        risultati.append((minEdge,node.element()))
                comm.send(risultati,dest=0)


                while True:
                    parent=comm.recv(source=0)
                    if parent==False:
                        break
                    successor_next=[-1]*len(parent)
                    for node in lista_nodi:
                        successor_next[node.element()]=parent[parent[node.element()]]

                    comm.send(successor_next,dest=0)


                lista_merge=comm.recv(source=0)


                lista_return_merge=[]

                lista_nodi_element=[x.element() for x in lista_nodi]

                t1=time.time()

                for node in lista_merge:
                    if node.root.element() in lista_nodi_element:
                        root=node.root
                        if node.root!=node:

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
                        else:
                            i=0
                            while i < len( node.listaArchi ):
                                edge = node.listaArchi[i]
                                n1, n2 = edge.endpoints()
                                if n1.element() == n2.element():
                                    node.listaArchi.pop( i )
                                else:
                                    i = i + 1

                        lista_return_merge.append(root)

                comm.send(lista_return_merge,dest=0)

                lista_nodi=lista_return_merge

















