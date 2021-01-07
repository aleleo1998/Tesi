import Sequenzial as sq
from Boruvka import Boruvka_parallel
import random
from Graph import Graph
import time as time
import copy


def creaRandom():
    g = Graph()
    for i in range(1000):
        g.insert_vertex( i )

    for node in g.vertices():
        for _ in range(2):
            peso = random.randint( 1, 10000000000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 99 )
            if nodo2 != node.element() and  g.peso_unico( peso ):

                e = g.insert_edge( node, g.vertices()[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )
    return g

if __name__=='__main__':
    g=creaRandom()
    g2=copy.deepcopy(g) #il grafo g viene modificato
    mst_parallel=Boruvka_parallel(g)
    mst_seq=sq.Boruvka_seq(g2)

    for edge in mst_seq.edges():
        n1,n2=edge.endpoints()
        e=mst_parallel.get_edge(mst_parallel.vertices()[n1.element()],mst_parallel.vertices()[n2.element()])
        if e is None:
            print("NON UGUALI")
            break

    if e is not None:
        print("UGUALI")

