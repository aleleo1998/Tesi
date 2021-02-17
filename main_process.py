from mpi4py import MPI
from Graph import Graph2, MST_PrimJarnik
from Graph2 import Graph
from random import randint
from time import time
import sys
from Boruvka_sequenziale import Boruvka_seq
from Boruvka_parallelo_array import Boruvka_parallel_array
from Boruvka_parallelo_queue import Boruvka_parallel_queue

import os


def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    g_sequenziale=Graph()
    g_parallel_array=Graph()
    g_parallel_queue=Graph()
    g_prim=Graph2()



    for i in range(10000):

        g_sequenziale.insert_vertex( i )
        g_parallel_queue.insert_vertex( i )
        g_parallel_array.insert_vertex( i )
        g_prim.insert_vertex(i)
        g.insert_vertex(i)

    nodi=g.vertices()
    nodi_sequenziale=g_sequenziale.vertices()
    nodi_array=g_parallel_array.vertices()
    nodi_queue=g_parallel_queue.vertices()
    nodi_prim=g_prim.vertices()

    for i in range(0,len(nodi)):
        while True:
            peso = randint( 1, 100000000 )
            if g.peso_unico(peso):
                if i+1==len(nodi):
                    g.insert_edge( nodi[i], nodi[0], peso )
                    g_prim.insert_edge(nodi_prim[i],nodi_prim[0],peso)
                    g_sequenziale.insert_edge(nodi_sequenziale[i],nodi_sequenziale[0],peso)
                    g_parallel_array.insert_edge(nodi_array[i],nodi_array[0],peso)
                    g_parallel_queue.insert_edge(nodi_queue[i],nodi_queue[0],peso)
                    break

                else:
                    g.insert_edge( nodi[i], nodi[i+1], peso )
                    g_prim.insert_edge(nodi_prim[i],nodi_prim[i+1],peso)
                    g_sequenziale.insert_edge(nodi_sequenziale[i],nodi_sequenziale[i+1],peso)
                    g_parallel_array.insert_edge(nodi_array[i],nodi_array[i+1],peso)
                    g_parallel_queue.insert_edge(nodi_queue[i],nodi_queue[i+1],peso)
                    break




    for node in nodi:
        i=0
        while i<499:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, 9999)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                g.insert_edge( node, nodi[nodo2], peso )
                g_sequenziale.insert_edge(nodi_sequenziale[node.element()],nodi_sequenziale[nodo2],peso)
                g_parallel_array.insert_edge(nodi_array[node.element()],nodi_array[nodo2],peso)
                g_parallel_queue.insert_edge(nodi_queue[node.element()],nodi_queue[nodo2],peso)
                g_prim.insert_edge(nodi_prim[node.element()],nodi_prim[nodo2],peso)
                i=i+1

    return g,g_sequenziale,g_parallel_array,g_parallel_queue,g_prim

def dividi_gruppi(lista_nodi, n):
    i = 0

    num = int( len( lista_nodi ) / n ) + 1
    cont = 0
    lista_return = [[] for _ in range( n )]
    lista_return_interi = [[] for _ in range( n )] # lista di ritorno
    hashmap={}
    while i < len( lista_nodi ):
        for _ in range( int( num ) ):
            if cont == n:
                cont = 0
            lista_return[cont].append(lista_nodi[i])
            hashmap[lista_nodi[i].posizione]=cont
            lista_return_interi[cont].append(lista_nodi[i].element())
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return (lista_return,hashmap,lista_return_interi)




def count_process(lista_divisa):
    n_process=0
    i=0
    while i<len(lista_divisa):
        l=lista_divisa[i]
        if l!=[]:
            n_process+=1
            i+=1
        else:
            lista_divisa.pop(i)
    return n_process
def pointer_jumping(parent,successor_next,n_process,processi_attivi):
    while True:
        change=False


        for i in range(n_process):
            if processi_attivi[i]:
                comm.send(parent,dest=i)
                successor=comm.recv(source=i)
                for i in range(len(successor)):
                    if successor[i]!=-1:
                        successor_next[i]=successor[i]



        for x,y in zip(parent,successor_next):
            if x!=y:
                change=True


                break

        if change==False:
            for i in range(n_process):
                if processi_attivi[i]:
                    comm.send(False, dest=i)
            break

        for i in range(len(parent)):
            parent[i]=successor_next[i]
def insert_min_edges(minimiArchi,lista_nodi_mst,mst,costo_mst):
    for result in minimiArchi:
        for edge,node in result:
            n1,n2=edge.endpoints_posizione()
            element1,element2=edge.endpoints()
            e=mst.insert_edge(lista_nodi_mst[n1]
                                 ,lista_nodi_mst[n2]
                                 ,edge.element())

            if element1==node:
                parent[node]=element2
            elif element2==node:
                parent[node]=element1

            if e is not None:
                costo_mst+=e.element()
    return costo_mst


if __name__=="__main__":
    g,g_seq,g_parallel_array,g_parallel_queue,g_prim=creaRandom()
    print("NUMERO DI NODI: {}, NUMERO DI ARCHI: {}".format(g.vertex_count(),g.edges_count()),flush=True)



    lista_nodi_originale=g.vertices()

    parent=[0 for node in g.vertices()]
    successor_next=[0 for node in g.vertices()]
    mst=Graph()


    for node in g.vertices():
        mst.insert_vertex(node.element())

    costo_mst=0



    lista_nodi_mst=mst.vertices()
    nodi_rimanenti=g.vertices()
    n_process=8
    comm = MPI.COMM_SELF.Spawn(sys.executable,args=[os.path.dirname(sys.argv[0])+"\child_processes.py"],
                           maxprocs=n_process)

    tempo_parallelo=time()
    cont_time=0
    processi_attivi=[True for i in range(n_process)]

    dict_nodi={}

    for node in lista_nodi_originale:
        dict_nodi[node.posizione]=node

    print("INIZIO MPI PARALLELO",flush=True)

    tempo_ricerca=0
    tempo_pointer_jumping=0
    tempo_merge=0
    dict_nodi={}

    while len(nodi_rimanenti)>1:
        for node in nodi_rimanenti:
            dict_nodi[node.posizione]=node

        lista_divisa,map,lista_interi=dividi_gruppi(nodi_rimanenti,n_process) #cambio

        tempo_ricerca_parziale=time()
        for i in range(len(lista_divisa)):
            if lista_divisa[i]==[]:
                processi_attivi[i]=False
                comm.send(False,dest=i)

        for i in range(len(lista_divisa)):
            if processi_attivi[i]:
                    comm.send((lista_divisa[i],map,processi_attivi),dest=i)

        minimiArchi=[]

        for i in range(len(processi_attivi)):
            if processi_attivi[i]:
                minimiArchi.append(comm.recv(source=i))

        costo_mst=insert_min_edges(minimiArchi,lista_nodi_mst,mst,costo_mst)
        tempo_ricerca+=(time()-tempo_ricerca_parziale)
        for i in range(len(parent)):
            opposto=parent[parent[i]]
            if i==opposto:
                if i<parent[i]:
                    parent[i]=i
                else:
                    parent[parent[i]]=parent[i]

        tempo_pointer_jumping_parziale=time()
        pointer_jumping(parent,successor_next,n_process,processi_attivi)
        tempo_pointer_jumping+=(time()-tempo_pointer_jumping_parziale)

        successor=[x for x in parent]

        for i in range(len(processi_attivi)):
            if processi_attivi[i]:
                comm.send(successor,dest=i)

        lista_merge=[[] for el in range(n_process)]

        tempo_merge_parziale=time()
        for i in range(n_process):
            if processi_attivi[i]:
                lista_recv_merge=comm.recv(source=i)
                for j,lista in enumerate(lista_recv_merge):
                    for el in lista:

                                lista_merge[j].append(dict_nodi[el])


        for i in range(n_process):
            if processi_attivi[i]:
                comm.send(lista_merge[i],dest=i)




        nodi_rimanenti=[]

        for i in range(len(processi_attivi)):
            if processi_attivi[i]:
                lista_rev=comm.recv(source=i)
                for node in lista_rev:
                    #dict_nodi[node.posizione]=node
                    nodi_rimanenti.append(node)


        tempo_merge+=(time()-tempo_merge_parziale)




    for i in range(len(processi_attivi)):
        if processi_attivi[i]:
            comm.send(False,dest=i)
    comm.Disconnect()

    tempo_parallelo=time()-tempo_parallelo
    #print("TEMPO PARALLELO",tempo_parallelo,flush=True)
    print("TEMPO RICERCA ARCHI MINIMI :{}, TEMPO POINTER JUMPING:{}, TEMPO MERGE:{}".format(tempo_ricerca,tempo_pointer_jumping,tempo_merge,flush=True))

    #print(costo_mst,cost_prim)


    print("INIZIO PARALLELO CON QUEUE",flush=True)
    tempo_queue=time()
    mst_queue,costo_queue=Boruvka_parallel_queue(g_parallel_queue)
    tempo_queue=time()-tempo_queue
    print("INIZIO PARALLELO CON ARRAY",flush=True)
    tempo_array=time()
    mst_array,costo_array=Boruvka_parallel_array(g_parallel_array)
    tempo_array=time()-tempo_array


    print("INIZIO SEQUENZIALE",flush=True)
    tempo_sequenziale=time()
    mst_seq,costo_sq=(0,0)#Boruvka_seq(g_seq)
    tempo_sequenziale=time()-tempo_sequenziale

    cost_prim=MST_PrimJarnik(g_prim)





    print( "Tempo parallelo mpi: {}, Tempo sequenziale: {}, \nTempo parallelo con array: {} , Tempo parallelo con queue: {}"
          "".format(tempo_parallelo,tempo_sequenziale,tempo_array,tempo_queue),flush=True)
    print("Costo mst parallelo mpi : {}, Costo mst sequenziale: {} ,\nCosto mst array: {}, Costo mst queue: {}, \nCosto mst prim: {}"
          "".format(costo_mst,costo_sq,costo_array,costo_queue,cost_prim),flush=True)
          















