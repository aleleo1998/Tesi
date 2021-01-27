from multiprocessing import Process,Array,JoinableQueue,Queue
from Graph import Graph
import time
import random





def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    for i in range(10000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        for _ in range(5):
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 9999 )
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )
    return g






def dividi_gruppi(lista_nodi, n,intero=False):
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
            if intero:
                lista_return[cont].append(lista_nodi[i].element())
            else:
                lista_return[cont].append(lista_nodi[i])
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return lista_return



def add_jobs(jobs, lista):
    for l in lista:
        jobs.put(l)


def jump_worker(jobs, parent, result):
    """
    Prima funzione parallela dell'algorito pointer_jumping
    :param jobs:
    :param parent:
    :param result:
    :return:
    """

    while True:
        group = jobs.get()

        lista_return=[None for i in range(len(parent))]
        for node in group:
            """
            il processo modifica la lista_result solo nelle posizioni dei nodi che ha ricevuto 
            il resto rimangono a None
            """
            lista_return[node]= parent[parent[node]]

        result.put(lista_return)

        jobs.task_done()


def jump_pro(n_pro, jobs, parent, result):
    for i in range( n_pro ):
        processi = Process( target=jump_worker, args=(jobs, parent, result) )
        processi.daemon = True
        processi.start()


def jump(parent, result,jobs):
    jump_pro( 8, jobs, parent, result )



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
            successor[node] = successor_next[node]
        jobs.task_done()


def copia_pro(n_pro, jobs, successor, successor_next):
    for i in range( n_pro ):
        processi = Process( target=copia_worker, args=(jobs, successor_next, successor) )
        processi.daemon = True
        processi.start()


def copia_successor(successor, successor_next, jobs3):

    copia_pro( 8, jobs3, successor, successor_next )


def worker_minimo(jobs, parent,result):
    while True:
        """
        Funzione parallela per trovare i minimi archi
        """

        lista_nodi = jobs.get()
        lista_min_edge=[]
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
            if n1 == node.element():
                parent[node.element()] = n2

            elif n2 == node.element():
                parent[node.element()] = n1
            lista_min_edge.append(minEdge)

        result.put(lista_min_edge)


        jobs.task_done()


def cerca_minimo_parallelo(n_pro, jobs, parent,result):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,result) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,jobs,result):
   cerca_minimo_parallelo( 2,jobs, parent,result )


def delete_edges(node):
    """
    Si vanno ad eliminare gli archi con lo stesso nome all'interno della lista
    degli archi della root
    :param node:
    :return:
    """
    i = 0

    while i < len( node.listaArchi ):
        edge = node.listaArchi[i]
        n1, n2 = edge.endpoints()
        if n1 == n2:
            node.listaArchi.pop( i )
        else:
            i = i + 1


def merge(node, root):
    """
    Inserire gli archi del nodo all'interno della lista archi della propria root.
    :param node:
    :param root:
    :return:


    Non ho bisogno di vedere se un arco è gia presente all'interno della lista
    degli archi della root, poichè il caso in cui due nodi voglio inserire lo stesso
    arco significa che hanno la stessa root e quindi hanno lo stesso nome, quindi l'arco
    viene eliminato direttamente.
    Altro caso in chi si vuole inserire lo stesso arco è quando il nodo ha un arco con la root,
    ma in questo caso i due nodi hanno lo stesso nome e quindi l'arco viene eliminato.

    """

    i = 0
    for edge in node.listaArchi:
        nodo1, nodo2 = edge.endpoints()
        if nodo1 != nodo2:
            root.listaArchi.append( edge )
            i = i + 1


def Boruvka_parallel(g):
    grafoB = Graph( False )

    lista_nodi = g.vertices()
    lista_nodi_originale=g.vertices()
    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )

    parent = Array( "i", g.vertex_count(), lock=False )
    result=Queue()

    peso_albero=0
    successor_next =Array( "i", g.vertex_count(), lock=False )

    jobs_min=JoinableQueue()
    minimo_paralelo(parent,jobs_min,result)

    jobs_jump=JoinableQueue()
    jump(parent,result,jobs_jump)

    jobs_copia=JoinableQueue()
    copia_successor(parent,successor_next,jobs_copia)

    lista_nodi_boruvka=grafoB.vertices()



    while len( lista_nodi ) > 1:

        lista_divisa = dividi_gruppi( lista_nodi, 8 )

        add_jobs(jobs_min,lista_divisa)
        jobs_min.join()

        while result.qsize()>0:
            list_min_edges=result.get()

            for edge in list_min_edges:
                node1,node2=edge.endpoints_posizione()
                e=grafoB.insert_edge(lista_nodi_boruvka[node1],
                                     lista_nodi_boruvka[node2]
                                     ,edge.element())

                if e is not None:
                    print("Iserisco",e)
                    peso_albero+=edge.element()



        """
        Se un nodo è il parent del suo parent allora lui e il suo parent hanno scelto
        lo stesso arco. Tra di loro il nodo con l'identificativo minimo gli viene
        settatto il parent a se stesso (diventa root).
        """
        for i in range( len( parent ) ):
            parent_opposto = parent[parent[i]]
            if i == parent_opposto:
                if i < parent[i]:
                    parent[i] = i
                else:
                    parent[parent[i]] = parent[i]

        #Algoritmo di wikipedia pointer_jumping
        lista_divisa_interi=dividi_gruppi(lista_nodi,8,True)
        while True:
            bool = True

            add_jobs(jobs_jump,lista_divisa_interi)
            jobs_jump.join()

            while result.qsize()>0:

                lista_return=result.get()
                for i,element in enumerate(lista_return):
                    if element is not None:
                        successor_next[i]=element


            for x, y in zip( parent, successor_next ):
                if x != y:
                    print( x, y )
                    bool = False
                    break

            if bool:
                break

            add_jobs(jobs_copia,lista_divisa_interi)
            jobs_copia.join()

        #aggiornamento del grafo origianale
        for j in range( len( parent ) ):
            nodo = lista_nodi_originale[j]
            nodo.root = lista_nodi_originale[parent[nodo.element()]]
            nodo.setElement( nodo.root.element() )  # il nodo prende il nome della root
            for edge in nodo.listaArchi:

                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])
        # il nodo prende il nome della root


        i = 0
        t=time.time()

        #merge delle liste degli archi all'interno delle liste degli archi delle root
        while i < len( lista_nodi ):
            node = lista_nodi[i]
            if node != node.root:
                merge( node, node.root )
                lista_nodi.remove( node )
            else:
                delete_edges(node)
                i = i + 1
        print("tempo merge",time.time()-t)


    return (grafoB,peso_albero)




if __name__ == '__main__':
    g=creaRandom()

    t1 = time.time()
    grafoB,peso_albero=Boruvka_parallel(g)

    #grafoB,peso_albero=Boruvka_parallel(g)



    print( "\nTEMPO DI ESECUZIONE", time.time() - t1 )






