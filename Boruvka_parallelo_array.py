from multiprocessing import Process,Array,JoinableQueue
from Graph import Graph
import time
from random import randint
import concurrent.futures





def jump_worker(parent,successor_next,jobs):
    while True:
        lista_nodi=jobs.get()
        for node in lista_nodi:
            successor_next[node]=parent[parent[node]]

        jobs.task_done()
def jump(n_pro,parent,successor_next,jobs):
    for i in range( n_pro ):
        process = Process( target=jump_worker, args=(parent,successor_next,jobs) )
        process.daemon = True
        process.start()
def jump_parallelo(parent,successor_next,jobs):
    jump( 8, parent,successor_next,jobs )


def copy_worker(parent,successor_next,jobs):
    while True:
        lista_nodi=jobs.get()
        for node in lista_nodi:

            parent[node]=successor_next[node]
        jobs.task_done()
def copy(n_pro,parent,successor_next,jobs):
    for i in range( n_pro ):
        process = Process( target=copy_worker, args=(parent,successor_next,jobs) )
        process.daemon = True
        process.start()
def copy_parallelo(parent,successor_next,jobs):
    copy( 8,parent, successor_next,jobs )


def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()

    for i in range(10000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        i=0
        while i<10:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, 9999 )
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is not None:
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
            posizione1,posizione2=minEdge.endpoints_posizione()
            """
            I controlli di seguito sono necessari poichè durante l'esecuzioni i nodi root
            avranno archi di cui tra le due estremità sarà presente un node che ha lo stesso nome della root
            ma potrebbe non essere la root stessa
            
            L'array modificato serve per tener traccia degli archi dei nomi dei nodi che
            sono l'estermita degli archi che si devono inserire.
            """
            if n1 == node.element():
                parent[node.element()] = n2
                modificato[posizione1]=posizione2
            elif n2 == node.element():
                modificato[posizione2]=posizione1
                parent[node.element()]=n1


        jobs.task_done()


def cerca_minimo_parallelo(n_pro, jobs, parent,modificato):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,modificato) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,jobs,modificato):

    cerca_minimo_parallelo( 8,jobs, parent,modificato )



def delete_edges(node):
    i = 0

    while i < len( node.listaArchi ):
        edge = node.listaArchi[i]
        n1, n2 = edge.endpoints()
        if n1 == n2:
            node.delete_edge(i)
        else:
            i = i + 1


def merge(node, root):
    i = 0
    while i < len(node.listaArchi):
        edge = node.listaArchi[i]
        nodo1, nodo2 = edge.endpoints()
        if nodo1 != nodo2:
            root.listaArchi.append(edge)
        i = i + 1


def Boruvka_parallel(g):
    grafoB = Graph( False )

    lista_nodi = g.vertices()
    lista_nodi_originale=g.vertices()
    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )
    parent = Array( "i", g.vertex_count(), lock=False )


    peso_albero=0
    successor_next =Array( "i", g.vertex_count(), lock=False )
    modificato=Array("i",g.vertex_count(),lock=False)

    jobs_min=JoinableQueue()
    minimo_paralelo(parent,jobs_min,modificato)

    jobs_jump=JoinableQueue()
    jump_parallelo(parent,successor_next,jobs_jump)

    jobs_copy=JoinableQueue()
    copy_parallelo(parent,successor_next,jobs_copy)

    lista_nodi_boruvka=grafoB.vertices()




    while len( lista_nodi ) > 1:
        for i in range(len(modificato)):
            modificato[i]=-1

        lista_divisa,lista_divisa_interi = dividi_gruppi( lista_nodi, 8 )
        add_jobs(jobs_min,lista_divisa)
        jobs_min.join()

        for j in range(len(modificato)):

            if modificato[j]!=-1:

                e=grafoB.insert_edge(lista_nodi_boruvka[j],lista_nodi_boruvka[modificato[j]]
                                     ,g.get_edge(lista_nodi_originale[j],lista_nodi_originale[modificato[j]]).element())
                if e is not None:
                    print("inserisco",e,flush=True)
                    peso_albero+=g.get_edge(lista_nodi_originale[j],lista_nodi_originale[modificato[j]]).element()


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
            change=False

            add_jobs(jobs_jump,lista_divisa_interi)
            jobs_jump.join()
            for x,y in zip(parent,successor_next):
                if x!=y:
                    print(x,y,flush=True)
                    change=True
                    break

            if change==False:
                break

            add_jobs(jobs_copy,lista_divisa_interi)
            jobs_copy.join()


        for j in range( len( parent ) ):
            nodo = lista_nodi_originale[j]
            nodo.root = lista_nodi_originale[parent[nodo.element()]]
            nodo.setElement( nodo.root.element() )  # il nodo prende il nome della root
            for edge in nodo.listaArchi:
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])

        i = 0
        t=time.time()
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



    print( "\nTEMPO DI ESECUZIONE", time.time() - t1 )



    #Controllo se ogni arco restuito dall'algoritmo di Prim sia presente nell'albero costruito.

