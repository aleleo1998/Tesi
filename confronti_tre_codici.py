
from multiprocessing import Array
from Boruvka_parallelo_array import Boruvka_parallel_array
from Boruvka_parallelo_queue import Boruvka_parallel_queue
from Graph2 import Graph
from random import randint
from Boruvka_sequenziale_esterno import Graph_seq

from time import time





def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO


    g_parallel_array=Graph()
    g_parallel_queue=Graph()
    dict_edge={}
    n=10000
    g_sequenziale=Graph_seq(n)

    for i in range(n):
        v1=g_parallel_queue.insert_vertex( i )
        v2=g_parallel_array.insert_vertex( i )
        v1.connessioni=Array("i",n,lock=False)
        v1.pesi_condivisi=Array("i",n,lock=False)
        v2.connessioni=Array("i",n,lock=False)
        v2.pesi_condivisi=Array("i",n,lock=False)


    nodi_array=g_parallel_array.vertices()
    nodi_queue=g_parallel_queue.vertices()


    for i in range(0,len(nodi_array)):
        while True:
            peso = randint( 1, 100000000 )
            if g_parallel_array.peso_unico(peso):
                if i+1==len(nodi_array):




                    e=g_parallel_array.insert_edge(nodi_array[i],nodi_array[0],peso)
                    dict_edge[e.element()]=e
                    g_parallel_queue.insert_edge(nodi_queue[i],nodi_queue[0],peso)
                    g_sequenziale.addEdge(i,0,peso)


                    nodi_queue[i].pesi_condivisi[nodi_queue[0].element()]=peso
                    nodi_queue[0].pesi_condivisi[nodi_queue[i].element()]=peso

                    nodi_queue[i].connessioni[len(nodi_queue[i].listaArchi)-1]=nodi_queue[0].element()
                    nodi_queue[0].connessioni[len(nodi_queue[0].listaArchi)-1]=nodi_queue[i].element()

                    nodi_array[i].pesi_condivisi[nodi_array[0].element()]=peso
                    nodi_array[0].pesi_condivisi[nodi_array[i].element()]=peso

                    nodi_array[i].connessioni[len(nodi_array[i].listaArchi)-1]=nodi_array[0].element()
                    nodi_array[0].connessioni[len(nodi_array[0].listaArchi)-1]=nodi_array[i].element()
                    break

                else:




                    e=g_parallel_array.insert_edge(nodi_array[i],nodi_array[i+1],peso)
                    dict_edge[e.element()]=e
                    g_parallel_queue.insert_edge(nodi_queue[i],nodi_queue[i+1],peso)
                    g_sequenziale.addEdge(i,i+1,peso)

                    nodi_queue[i].pesi_condivisi[nodi_queue[i+1].element()]=peso
                    nodi_queue[i+1].pesi_condivisi[nodi_queue[i].element()]=peso
                    nodi_queue[i].connessioni[len(nodi_queue[i].listaArchi)-1]=nodi_queue[i+1].element()
                    nodi_queue[i+1].connessioni[len(nodi_queue[i+1].listaArchi)-1]=nodi_queue[i].element()

                    nodi_array[i].pesi_condivisi[nodi_array[i+1].element()]=peso
                    nodi_array[i+1].pesi_condivisi[nodi_array[i].element()]=peso
                    nodi_array[i].connessioni[len(nodi_array[i].listaArchi)-1]=nodi_array[i+1].element()
                    nodi_array[i+1].connessioni[len(nodi_array[i+1].listaArchi)-1]=nodi_array[i].element()
                    break




    for node in nodi_array:
        i=0
        while i<1:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, n-1)
            if nodo2 != node.element() and g_parallel_array.get_edge(node,nodi_array[nodo2]) is None and g_parallel_array.peso_unico(peso):




                e=g_parallel_array.insert_edge(nodi_array[node.element()],nodi_array[nodo2],peso)
                dict_edge[e.element()]=e
                g_parallel_queue.insert_edge(nodi_queue[node.element()],nodi_queue[nodo2],peso)
                g_sequenziale.addEdge(node.element(),nodo2,peso)


                nodi_queue[node.element()].pesi_condivisi[nodi_queue[nodo2].element()]=peso
                nodi_queue[nodo2].pesi_condivisi[nodi_queue[node.element()].element()]=peso
                nodi_queue[node.element()].connessioni[len(nodi_queue[node.element()].listaArchi)-1]=nodi_queue[nodo2].element()
                nodi_queue[nodo2].connessioni[len( nodi_queue[nodo2].listaArchi)-1]=nodi_queue[node.element()].element()


                nodi_array[node.element()].pesi_condivisi[nodi_array[nodo2].element()]=peso
                nodi_array[nodo2].pesi_condivisi[nodi_array[node.element()].element()]=peso
                nodi_array[node.element()].connessioni[len(nodi_array[node.element()].listaArchi)-1]=nodi_array[nodo2].element()
                nodi_array[nodo2].connessioni[len( nodi_array[nodo2].listaArchi)-1]=nodi_array[node.element()].element()
                i=i+1


    lista_pesi_condivisi_queue=[]
    lista_connessioni_condivise_queue=[]
    lista_connessioni_condivise_array=[]
    lista_pesi_condivisi_array=[]
    for node in nodi_array:
        if len(node.listaArchi)<n:
            node.connessioni[len(node.listaArchi)]=-1

        lista_pesi_condivisi_array.append(node.pesi_condivisi)
        lista_connessioni_condivise_array.append(node.connessioni)

    for node in nodi_queue:
        if len(node.listaArchi)<n:
            node.connessioni[len(node.listaArchi)]=-1

        lista_pesi_condivisi_queue.append(node.pesi_condivisi)
        lista_connessioni_condivise_queue.append(node.connessioni)

    return g_parallel_array,g_parallel_queue,g_sequenziale, \
           lista_pesi_condivisi_array, \
           lista_connessioni_condivise_array,lista_pesi_condivisi_queue,lista_connessioni_condivise_queue,dict_edge



if __name__=="__main__":
    g_parallel_array,g_parallel_queue,g_sequenziale, \
    lista_pesi_condivisi_array, \
    lista_connessioni_condivise_array,lista_pesi_condivisi_queue,lista_connessioni_condivise_queue,dict_edge=creaRandom()
    print("INiZIO",flush=True)

    t_array=time()
    mst_array,peso_array=Boruvka_parallel_array(g_parallel_array,
                                                 lista_pesi_condivisi_array,lista_connessioni_condivise_array,dict_edge)
    t_array=time()-t_array
    print("PESO MST ARRAY",peso_array,flush=True)

    t_queue=time()
    mst_queue,peso_queue=Boruvka_parallel_queue(g_parallel_queue,
                                                lista_pesi_condivisi_queue,lista_connessioni_condivise_queue,dict_edge)
    t_queue=time()-t_queue
    print("PESO MST QUEUE",peso_queue,flush=True)

    t_seq=time()
    peso_seq=g_sequenziale.boruvkaMST()
    t_seq=time()-t_seq

    print("PESO SEQUENZIALE",peso_seq)

    print("TEMPO ARRAY: {}, TEMPO QUEUE: {}, TEMPO SEQUENZIALE: {}".format(t_array,t_queue,t_seq))