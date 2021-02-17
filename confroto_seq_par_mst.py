import Boruvka_sequenziale as sq
from Boruvka_parallelo_array import Boruvka_parallel_array
from Boruvka_parallelo_queue import Boruvka_parallel_queue
import random
from Graph2 import Graph

import time as time
import concurrent.futures






def creaRandom():
    g = Graph()
    for i in range(3000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        i=0
        while i<10:
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 2999)
            if nodo2 != node.element(): #and g.get_edge(node,nodi[nodo2]) is None: #and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is not None:
                    #print("inserisco")
                    i+=1

    return g

import sys

if __name__=='__main__':
    g=creaRandom()



    

    g2=Graph()
    for node in g.vertices():
        g2.insert_vertex(node.element())
    for edge in g.edges():
        n1,n2=edge.endpoints()
        g2.insert_edge(g2.vertices()[n1],g2.vertices()[n2],edge.element())

    t=time.time()
    mst_parallel,peso_pa=Boruvka_parallel(g)
    tempo_paralleo=time.time()-t

    t=time.time()
    mst_seq,peso_sq=sq.Boruvka_seq(g2)
    tempo_sequenziale=time.time()-t

    for edge in mst_seq.edges():
        n1,n2=edge.endpoints_posizione()
        e=mst_parallel.get_edge(mst_parallel.vertices()[n1],mst_parallel.vertices()[n2])
        if e is None:
            print("NON UGUALI")
            print("Peso mst parallelo",peso_pa,"Peso mst sequenzaile",peso_sq)
            break

    if e is not None:
        print("UGUALI")
        print( "Tempo parallelo: {}, Tempo sequenziale: {}".format(tempo_paralleo,tempo_sequenziale))
        print("Peso mst parallelo",peso_pa,"Peso mst sequenzaile",peso_sq)


