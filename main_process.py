from mpi4py import MPI
from Graph import *
from random import randint
from time import time
from multiprocessing import Array
import sys
import concurrent.futures


def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()

    for i in range(10000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        i=0
        while i<2:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, 9999 )
            if nodo2 != node.element(): #and g.get_edge(node,nodi[nodo2]) is None: #and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is not None:
                    i+=1

    return g

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

def pointer_jumping(parent,successor_next,lista_divisa):
    while True:
        change=False
        print("ITERAZIONE POINTER JUMPING",flush=True)

        jump(parent,successor_next,lista_divisa)

        for x,y in zip(parent,successor_next):
            if x!=y:

                change=True
                break

        if change==False:
            break

        copy(parent,successor_next,lista_divisa)

def insert_min_edges(minimiArchi,lista_nodi_grafoB,grafoB,peso_albero):
    for result in minimiArchi:
        for edge,node in result:
            n1,n2=edge.endpoints_posizione()
            element1,element2=edge.endpoints()
            e=grafoB.insert_edge(lista_nodi_grafoB[n1]
                                 ,lista_nodi_grafoB[n2]
                                 ,edge.element())

            if element1==node:
                parent[node]=element2
            elif element2==node:
                parent[node]=element1

            if e is not None:
                peso_albero+=e.element()
                print("Inserisco",e,flush=True)
    return peso_albero


if __name__=="__main__":
    g=creaRandom()
    parent=Array("i",g.vertex_count(),lock=False)
    successor_next=Array("i",g.vertex_count(),lock=False)

    grafoB=Graph()
    for node in g.vertices():
        grafoB.insert_vertex(node.element())
    peso_albero=0

    lista_nodi_grafoB=grafoB.vertices()
    nodi_rimanenti=g.vertices()

    t=time()

    #INIZIO ALGORITMO
    while len(nodi_rimanenti)>1:
        lista_divisa,mapp=dividi_gruppi(nodi_rimanenti,8)
        n_process=count_process(lista_divisa)
        comm = MPI.COMM_SELF.Spawn(sys.executable,args=['C:/Users/alexl/Desktop/TesiGit/child_processes.py'],
                               maxprocs=n_process)

        for i in range(n_process):
                comm.send((lista_divisa[i],mapp),dest=i)

        minimiArchi=[]

        for i in range(n_process):
            minimiArchi.append(comm.recv(source=i))

        peso_albero=insert_min_edges(minimiArchi,lista_nodi_grafoB,grafoB,peso_albero)

        for i in range(len(parent)):
            opposto=parent[parent[i]]
            if i==opposto:
                if i<parent[i]:
                    parent[i]=i
                else:
                    parent[parent[i]]=parent[i]

        pointer_jumping(parent,successor_next,lista_divisa)

        successor=[x for x in parent]

        for i in range(n_process):
            comm.send(successor,dest=i)

        nodi_rimanenti=[]

        for i in range(n_process):
            lista_rev=comm.recv(source=i)
            for node in lista_rev:
                nodi_rimanenti.append(node)



    print( "\nTEMPO DI ESECUZIONE", time()-t,flush=True )









