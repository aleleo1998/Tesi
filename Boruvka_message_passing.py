from Graph import *
from mpi4py import MPI
import random
import sys
import time
import copy
from multiprocessing import Array


import concurrent.futures

sys.setrecursionlimit(200000)

def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    for i in range(10000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        i=0
        while i<10:
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 9999 )
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

def get_job(lista_divisa):
    for l in lista_divisa:
        yield l
def jump_worker(l,parent,successor_next):
    for node in l:
        successor_next[node.element()]=parent[parent[node.element()]]





def jump(parent,successor_next,lista_divisa):
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for l in get_job(lista_divisa):
            executor.submit(jump_worker,l,parent,successor_next)


def copy_worker(l,parent,successor_next):
    for node in l:

        parent[node.element()]=successor_next[node.element()]




def copy(parent,successor_next,lista_divisa):
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for l in get_job(lista_divisa):
            executor.submit(copy_worker,l,parent,successor_next)



def dividi_gruppi(lista_nodi, n):
    i = 0

    hashmap={}
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
            hashmap[lista_nodi[i].posizione]=cont
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return lista_return,hashmap

if __name__=="__main__":
    comm = MPI.COMM_WORLD#canale di comunicazione
    rank = comm.Get_rank() #ogni processo ha il suo rank
    size=comm.Get_size() #numero di processi


    if rank == 0:


        g=creaRandom()
        #g=creaGrafo()
        parent=Array("i",g.vertex_count(),lock=False)
        successor_next=Array("i",g.vertex_count(),lock=False)

        if g.iscon():
            grafoB=Graph()
            for i in range(g.vertex_count()):
                grafoB.insert_vertex(i)


            lista_nodi_grafoB=grafoB.vertices()
            nodi_rimanenti=g.vertices()

            tempo_finale=time.time()

            processi_invio=[True for x in range(0,size)]

            nodi_rimanenti=g.vertices()
            while len(nodi_rimanenti)>1:
                lista_divisa,mapp=dividi_gruppi(nodi_rimanenti,size-1)
                for i,l in enumerate(lista_divisa):
                    if l==[]:
                        processi_invio[i+1]=False
                        comm.send((False,None,None),dest=i+1)
                for i in range(1,size):

                    if processi_invio[i]:
                            comm.send((lista_divisa[i-1],mapp,processi_invio),dest=i)

                minimiArchi=[]

                #minimi archi ricevuti dai processi
                for i in range(1,size):
                    if processi_invio[i]:
                        minimiArchi.append(comm.recv(source=i))

                #si riceve una lista di liste
                for r in minimiArchi:
                    for edge,node in r:
                        n1,n2=edge.endpoints()
                        e=grafoB.insert_edge(lista_nodi_grafoB[n1.posizione]
                                             ,lista_nodi_grafoB[n2.posizione]
                                             ,edge.element())
                        if n1.element==node:
                            parent[node]=n2.element
                        elif n2.element==node:
                            parent[node]=n1.element

                        if e is not None:
                           print("Inserisco",e,flush=True)



                for i in range(len(parent)):
                    opposto=parent[parent[i]]

                    if i==opposto:

                        if i<parent[i]:
                            parent[i]=i
                        else:
                            parent[parent[i]]=parent[i]



                while True:
                    change=False
                    jump(parent,successor_next,lista_divisa)

                    for x,y in zip(parent,successor_next):
                        if x!=y:
                            print(x,y,flush=True)
                            change=True
                            break

                    if change==False:
                        break

                    copy(parent,successor_next,lista_divisa)

                successor=[x for x in parent]





                #invio parent a tutti gli altri processi in modo tale da conoscere la root
                # dei nodi che possiedono
                for i in range(1,size):
                    if processi_invio[i]:
                        comm.send(successor,dest=i)



                nodi_rimanenti=[]

                for i in range(1,size):
                    if processi_invio[i]:
                        lista_rev=comm.recv(source=i)
                        for node in lista_rev:
                            nodi_rimanenti.append(node)

            for i in range(1,size):
                if processi_invio[i]:
                    comm.send((False,False,None),dest=i)























            #(grafoB.iscon())
            print("tempo",time.time()-tempo_finale,flush=True)
            print(grafoB.edge_count(),flush=True)


            peso_prim=0
            for edge in g.MST_PrimJarnik():
                n1, n2 = edge.endpoints()
                peso_prim+=edge.element()
                e = grafoB.get_edge( grafoB.vertices()[n1.posizione], grafoB.vertices()[n2.posizione] )
                if e is None:
                    print( "ERRORE NELLA COSTRUZIONE DEL MST" )
                    break

            if e is not None:

                print( "L'albero costruito è minimo con peso {} da grafo con nodi {} e archi {}".format(peso_prim,g.vertex_count(),g.edge_count()) )

    elif rank!=0:

        #riceve la lista di nodi dal processo 0 (lo considero come quello principale).



        while True:

            lista_nodi,mapp,processi_invio=comm.recv(source=0)
            if lista_nodi==False:

                break



            risultati=[]

            #ricerca degli archi minimi e invio al processo 0
            for node in lista_nodi:
                minEdge=None
                for edge in node.listaArchi:
                    if minEdge is None or minEdge.element()>edge.element():
                        minEdge=edge


                if minEdge is not None:
                    risultati.append((minEdge,node.element()))
            comm.send(risultati,dest=0)


            # pointer_jumping in paralello. Si aggiorna i parent dei nodi che si possiendo e si invia

            # al processo 0


            # riceve il parent aggiornato
            parent=comm.recv(source=0)












            # si aggiornano i nomi dei nodi che possiede il processo e i nomi
            # dei nodi che fanno parte degli archi del nodo.
            lista_nodi_element=[x.element() for x in lista_nodi]


            for node in lista_nodi:
                node.setElement(parent[node.element()])
                node.root=parent[node.element()]
                for edge in node.listaArchi:
                    n1,n2=edge.endpoints()
                    n1.root=parent[n1.element]
                    n2.root=parent[n2.element]
                    n1.setElement(parent[n1.element])
                    n2.setElement(parent[n2.element])


            lista_merge=[]






            lista_inv=[[] for x in range(1,size)]


            for node in lista_nodi:
                if mapp[node.root]!=(rank-1):

                    lista_inv[mapp[node.root]].append(node)
                else:
                    lista_merge.append(node)



            for i in range(1,size):
                if i!=rank:
                    if processi_invio[i]:
                        comm.send(lista_inv[i-1],dest=i)



            for i in range(1,size):
                if i!=rank:
                    if processi_invio[i]:
                        lista_recv=comm.recv(source=i)
                        for node in lista_recv:
                            lista_merge.append(node)








            lista_return_merge=[]


            #Si scandisce la lista_merge e la lista di ogni nodo viene inserita nella lista
            # degli archi della root. Se invece il nodo è una root si vanno solo ad eliminare
            #gli archi con lo stesso nome.
            for node in lista_merge:

                if node.posizione!=node.root:
                    root=lista_nodi[lista_nodi_element.index(node.root)]

                    i = 0
                    while i < len(node.listaArchi):
                        edge = node.listaArchi[i]
                        nodo1, nodo2 = edge.endpoints()
                        if nodo1.element != nodo2.element:

                            root.listaArchi.append( edge )
                            i = i + 1
                        else:

                            if node.posizione == nodo1.posizione or node.posizione == nodo2.posizione:  # se tra le due estermità non è presente il node in input alla funzione non serve cancellare l'arco dalla sua lista
                                node.listaArchi.pop(i)
                            else:
                                i = i + 1

                    if root not in lista_return_merge:
                        lista_return_merge.append(root)
                else:
                    node=lista_nodi[lista_nodi_element.index(node.element())]
                    i=0
                    while i < len( node.listaArchi ):
                        edge = node.listaArchi[i]
                        n1, n2 = edge.endpoints()
                        if n1.element == n2.element:

                            node.listaArchi.pop( i )
                        else:

                            i = i + 1
                    if node not in lista_return_merge:
                        lista_return_merge.append(node)


            comm.send(lista_return_merge,dest=0)



