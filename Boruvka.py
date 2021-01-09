from multiprocessing import Process,Array,JoinableQueue
from Graph import Graph
import collections
import time
import random
import sys



sys.setrecursionlimit(20000)
def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    for i in range( 2000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        for _ in range(1):
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 1999 )
            if nodo2 != node.element() and g.peso_unico( peso ):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )
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



def add_jobs(jobs, lista):
    for l in lista:
        jobs.put(l)


def jump_worker(jobs, parent, successor_next):
    """
    Prima funzione parallela dell'algorito pointer_jumping
    :param jobs:
    :param parent:
    :param result:
    :return:
    """

    while True:
        group = jobs.get()
        for node in group:
            """
            il processo modifica la lista_result solo nelle posizioni dei nodi che ha ricevuto 
            il resto rimangono a None
            """
            successor_next[node.element()] = parent[parent[node.element()]]

        jobs.task_done()


def jump_pro(n_pro, jobs, parent, successor_next):
    for i in range( n_pro ):
        processi = Process( target=jump_worker, args=(jobs, parent, successor_next) )
        processi.daemon = True
        processi.start()


def jump(parent, successor_next,jobs):
    #jobs = multiprocessing.JoinableQueue()
    #add_jobs( jobs, lista_div )
    jump_pro( 8, jobs, parent, successor_next )
    #jobs.join()


def copia_worker(jobs, successor_next, successor):
    """
    Seconda funzione parallela del pointer_jumping
    :param jobs:
    :param successor_next:
    :param successor:
    :return:
    """
    while True:

        group = jobs.get()
        for node in group:
            successor[node.element()] = successor_next[node.element()]
        jobs.task_done()


def copia_pro(n_pro, jobs, successor, successor_next):
    for i in range( n_pro ):
        processi = Process( target=copia_worker, args=(jobs, successor_next, successor) )
        processi.daemon = True
        processi.start()


def copia_successor(successor, successor_next, jobs3):
    #jobs3 = multiprocessing.JoinableQueue()
    #add_jobs( jobs3, lista_divisa )
    copia_pro( 8, jobs3, successor, successor_next )
    #jobs3.join()


def worker_minimo(jobs, parent,modificato):
    while True:
        """
        Funzione parallela per trovare i minimi archi
        """

        lista_nodi = jobs.get()
        for node in lista_nodi:
            min = -1
            minEdge = None

            for edge in node.listaArchi:
                if min == -1 or min > edge.element():
                    minEdge = edge
                    min = edge.element()
            if minEdge is None:
                raise ValueError( "La lista del nodo in input è vuota" )
            n1, n2 = minEdge.endpoints()
            """
            I controlli di seguito sono necessari poichè durante l'esecuzioni i nodi root
            avranno archi di cui tra le due estremità sarà presente un node che ha lo stesso nome della root
            ma potrebbe non essere la root stessa
            """
            if n1.element() == node.element():
                parent[node.element()] = n2.element()
                modificato[n1.posizione]=n2.posizione
            elif n2.element() == node.element():
                modificato[n2.posizione]=n1.posizione
                parent[node.element()] = n1.element()


        jobs.task_done()


def cerca_minimo_parallelo(n_pro, jobs, parent,modificato):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,modificato) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,jobs,modificato):
    #jobs = multiprocessing.JoinableQueue()
    cerca_minimo_parallelo( 8,jobs, parent,modificato )
    #add_jobs( jobs, lista_divisa )
    #jobs.join()


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
        if n1.element() == n2.element():
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
        if nodo1.element() != nodo2.element():
            root.listaArchi.append( edge )
            i = i + 1
        else:
            if node == nodo1 or node == nodo2:  # se tra le due estermità non è presente il node in input alla funzione non serve cancellare l'arco dalla sua lista
                node.listaArchi.pop(i)
            else:
                i = i + 1


def Boruvka_parallel(g):
    grafoB = Graph( False )

    lista_nodi = g.vertices()
    lista_nodi_originale=g.vertices()
    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )
    parent = Array( "i", g.vertex_count(), lock=False )
    #result=multiprocessing.Queue()

    peso_albero=0
    successor_next =Array( "i", g.vertex_count(), lock=False )
    modificato=Array("i",g.vertex_count(),lock=False)
    jobs_min=JoinableQueue()
    minimo_paralelo(parent,jobs_min,modificato)
    jobs_jump=JoinableQueue()
    jump(parent,successor_next,jobs_jump)

    jobs_copia=JoinableQueue()
    copia_successor(parent,successor_next,jobs_copia)
    lista_nodi_boruvka=grafoB.vertices()

    if g.iscon():

        while len( lista_nodi ) > 1:
            for i in range(len(modificato)):
                modificato[i]=-1

            lista_divisa = dividi_gruppi( lista_nodi, 8 )
            #print(lista_divisa)
            add_jobs(jobs_min,lista_divisa)
            jobs_min.join()

            for j in range(len(modificato)):

                if modificato[j]!=-1:

                    e=grafoB.insert_edge(lista_nodi_boruvka[j],lista_nodi_boruvka[modificato[j]]
                                         ,g.get_edge(lista_nodi_originale[j],lista_nodi_originale[modificato[j]]).element())

                    if e is not None:
                        print(e)
                        peso_albero+=g.get_edge(lista_nodi_originale[j],lista_nodi_originale[modificato[j]]).element()

            """
            while result.qsize()>0:
                lista_edge = result.get()
                for edge in lista_edge:
                    nodo1, nodo2 = edge.endpoints()
                    if grafoB.get_edge( grafoB.vertices()[nodo1.posizione], grafoB.vertices()[nodo2.posizione] ) is None:
                        grafoB.insert_edge( grafoB.vertices()[nodo1.posizione], grafoB.vertices()[nodo2.posizione],
                                            edge.element() )
                        peso_albero+=edge.element()
                        print( "inserisco arco:({},{},{})".format( nodo1.posizione, nodo2.posizione, edge.element() ) )

            print(time.time()-t1)
         

        
            Se due nodi sono reciprocamente uno il parent dell'altro 
            si va a controllare qual ha l'indentificativo minore, quest'ultimo
            avrà come parent se stesso (diventa root)
            """


            for i in range( len( parent ) ):
                parent_opposto = parent[parent[i]]
                if i == parent_opposto:
                    if i < parent[i]:
                        parent[i] = i
                    else:
                        parent[parent[i]] = parent[i]

                    """
                    Algoritmo di wikipedia (pointer_jumping) di seguito
                    """
            while True:
                bool = True

                add_jobs(jobs_jump,lista_divisa)
                jobs_jump.join()


                """
                Si vanno a copiare tutte le liste contenunte nella Queue in successor_next.
                Ogni processo ha modificato solo le posizioni dei nodi avuti in input, quindi all'interno delle 
                liste possono essere presenti più posizioni a None
                """



                for x, y in zip( parent, successor_next ):
                    if x != y:
                        print( x, y )
                        bool = False
                        break
                if bool:
                    break
                add_jobs(jobs_copia,lista_divisa)
                jobs_copia.join()

            """
            Aggiornamento dei nodi del grafo originale.
            """

            for j in range( len( parent ) ):
                nodo = lista_nodi_originale[j]
                nodo.root = lista_nodi_originale[parent[nodo.element()]]
                nodo.setElement( nodo.root.element() )  # il nodo prende il nome della root

            i = 0
            """
            Merge delle liste
            """
            while i < len( lista_nodi ):
                node = lista_nodi[i]
                if node != node.root:
                    merge( node, node.root )
                    lista_nodi.remove( node )
                else:
                    delete_edges(node)
                    i = i + 1

            """
            Nella lista_nodi resteranno solo i nodi che sono root.
            Se la lista_nodi conterrà soltano un nodo significherà che avremo solo una 
            root e quinidi si può terminare
            """
        return (grafoB,peso_albero)
    else:
        print( "GRAFO PASSATO IN INPUT NON CONNESSO" )
        return None



if __name__ == '__main__':
    #g=creaGrafo()
    g=creaRandom()

    t1 = time.time()
    grafoB,peso_albero=Boruvka_parallel(g)

    #grafoB,peso_albero=Boruvka_parallel(g)



    print( "\nTEMPO DI ESECUZIONE", time.time() - t1 )

    if grafoB.iscon():
        print( "\nAlbero conesso" )


    #Controllo se ogni arco restuito dall'algoritmo di Prim sia presente nell'albero costruito.

    peso_prim=0
    for edge in g.MST_PrimJarnik():
        n1, n2 = edge.endpoints()
        peso_prim+=edge.element()
        e = grafoB.get_edge( grafoB.vertices()[n1.posizione], grafoB.vertices()[n2.posizione] )
        if e is None:
            print( "ERRORE NELLA COSTRUZIONE DEL MST" )
            break

    if e is not None:
        print( "L'albero costruito è minimo con peso",peso_albero,peso_prim )

    print( "Numero di archi deve essere n-1 ({}):".format( grafoB.vertex_count() - 1 ) )
    print( "Bouruvka costruito:", grafoB.edge_count(), "Prim:", len( g.MST_PrimJarnik() ) )

