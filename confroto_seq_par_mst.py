import Sequenzial as sq
from Boruvka import Boruvka_parallel
import random
from Graph import Graph
import time as time
import concurrent.futures






def creaRandom():
    g = Graph()
    for i in range(2000):
        g.insert_vertex( i )
    
    lista_nodi=g.vertices()
    for node in lista_nodi:

        for _ in range(2):

            peso = random.randint( 1, 10000000000000)
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO

            nodo2 = random.randint( 0,1999 )

            if nodo2 != node.element(): #and g.peso_unico( peso ):

                e = g.insert_edge( node, lista_nodi[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )


    return g


import sys

if __name__=='__main__':
    g=creaRandom()



    

    g2=Graph()
    for node in g.vertices():
        g2.insert_vertex(node.element())
    for edge in g.edges():
        n1,n2=edge.endpoints()
        g2.insert_edge(g2.vertices()[n1.element()],g2.vertices()[n2.element()],edge.element())
    t=time.time()
    mst_parallel,peso_pa=Boruvka_parallel(g)
    tempo_paralleo=time.time()-t

    t=time.time()
    mst_seq,peso_sq=sq.Boruvka_seq(g2)
    tempo_sequenziale=time.time()-t

    for edge in mst_seq.edges():
        n1,n2=edge.endpoints()
        e=mst_parallel.get_edge(mst_parallel.vertices()[n1.posizione],mst_parallel.vertices()[n2.posizione])
        if e is None:
            print("NON UGUALI")
            break

    if e is not None:
        print("UGUALI")
        print("Numero di vertici {}, Numero di archi {}, "
              "Tempo parallelo: {}, Tempo sequenziale: {}".format(g.vertex_count(),g.edge_count(),tempo_paralleo,tempo_sequenziale))
        print("Peso mst parallelo",peso_pa,"Peso mst sequenzaile",peso_sq)


