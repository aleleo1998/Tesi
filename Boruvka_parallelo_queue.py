from multiprocessing import Process,Array,JoinableQueue,Queue
from Graph2 import Graph
import time
from random import randint





def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()




    for i in range(2000):
        g.insert_vertex(i)

    nodi=g.vertices()


    for i in range(1,len(nodi)):
        while True:
            peso = randint( 1, 100000000 )
            if g.peso_unico(peso):
                g.insert_edge( nodi[i-1], nodi[i], peso )
                break



    for node in nodi:
        for _ in range(100):
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, 1999)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                g.insert_edge( node, nodi[nodo2], peso )


    return g


def dividi_gruppi(lista_nodi, n):
    i = 0

    num = int( len( lista_nodi ) / n ) + 1
    cont = 0
    lista_return = [[] for _ in range( n )]
    lista_return_interi = [[] for _ in range( n )] # lista di ritorno

    while i < len( lista_nodi ):
        for _ in range( int( num ) ):
            if cont == n:
                """
                il contatore cont riparte da zero in modo tale da avere una distribuzione uniforme
                dei nodi sulle varie liste
                """
                cont = 0
            lista_return[cont].append(lista_nodi[i])

            lista_return_interi[cont].append(lista_nodi[i].element())
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return (lista_return,lista_return_interi)








def add_jobs(jobs, lista):
    for l in lista:
        jobs.put(l)


def jump_worker(jobs, parent, result):


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


        lista_nodi = jobs.get()
        lista_min_edge=[]
        for node in lista_nodi:
            min = -1
            minEdge = None

            for edge in node.incident_edges():
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

def merge(node, root):
    edges=node.listaArchi.copy()
    for edge in edges.values():
        nodo1, nodo2 = edge.endpoints()
        if nodo1 != nodo2:
            if nodo1==root.element():
                root.add_arco_root(nodo2,edge)
            else:
                root.add_arco_root(nodo1,edge)

def delete_multi_edges(lista_nodi):
    for node in lista_nodi:
        dict_edge=node.listaArchi.copy()
        for key,edge in dict_edge.items():
            n1,n2=edge.endpoints()
            if n1==n2:
                node.delete_edge(key)

            elif n1==node.element():

                if n2!=key:
                    node.add_arco_root(n2,edge)
                    node.delete_edge(key)
            elif n2==node.element():
                if n1!=key:
                    node.add_arco_root(n1,edge)
                    node.delete_edge(key)


def Boruvka_parallel_queue(g):
    grafoB = Graph( )

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

        lista_divisa,lista_divisa_interi = dividi_gruppi( lista_nodi, 8 )

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

                    bool = False
                    break

            if bool:
                break

            add_jobs(jobs_copia,lista_divisa_interi)
            jobs_copia.join()

        #aggiornamento del grafo origianale
        for node in lista_nodi:
            node.root = parent[node.element()]
            node.setElement(node.root)  # il nodo prende il nome della root
            for edge in node.incident_edges():
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])

        i = 0
        while i < len( lista_nodi ):
            node = lista_nodi[i]
            if node.posizione != node.root:
                merge( node,lista_nodi_originale[node.root] )
                lista_nodi.remove( node )
            else:

                i = i + 1

        delete_multi_edges(lista_nodi)



    return (grafoB,peso_albero)




if __name__ == '__main__':
    g=creaRandom()

    t1 = time.time()
    grafoB,peso_albero=Boruvka_parallel_queue(g)


    print( "\nTEMPO DI ESECUZIONE", time.time() - t1 )






