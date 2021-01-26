from multiprocessing import Process,Array,JoinableQueue
from Graph import Graph
import time
import random
"""
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
"""



def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    for i in range(20000):

        g.insert_vertex( i )

    nodi=g.vertices()
    for node in nodi:
        for _ in range(10):
            peso = random.randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 9999 )
            if nodo2 != node.element():# and g.peso_unico( peso ):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is None:
                    print( "non inserisco" )
                else:
                    print( "Inserisco" )
            else:
                print( "non inserisco" )
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
            
            L'array modificato serve per tener traccia degli archi dei nomi dei nodi che
            sono l'estermita degli archi che si devono inserire.
            """
            if n1.element == node.element():
                parent[node.element()] = n2.element
                modificato[n1.posizione]=n2.posizione
            elif n2.element == node.element():
                modificato[n2.posizione]=n1.posizione
                parent[node.element()] = n1.element


        jobs.task_done()


def cerca_minimo_parallelo(n_pro, jobs, parent,modificato):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,modificato) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,jobs,modificato):
    cerca_minimo_parallelo( 8, jobs, parent, modificato )



def jump_worker(parent,successor_next,jobs):
    while True:
        lista_nodi=jobs.get()
        for node in lista_nodi:
            i=node.element()
            successor_next[i]=parent[parent[i]]

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
            i=node.element()
            parent[i]=successor_next[i]
        jobs.task_done()
def copy(n_pro,parent,successor_next,jobs):
    for i in range( n_pro ):
        process = Process( target=copy_worker, args=(parent,successor_next,jobs) )
        process.daemon = True
        process.start()
def copy_parallelo(parent,successor_next,jobs):
    copy( 8,parent, successor_next,jobs )



def delete_edges(node):
    i = 0

    while i < len( node.listaArchi ):
        edge = node.listaArchi[i]
        n1, n2 = edge.endpoints()
        if n1.element == n2.element:
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

    """
    Non ho bisogno di vedere se un arco è gia presente all'interno della lista
    degli archi della root, poichè il caso in cui due nodi voglio inserire lo stesso
    arco significa che hanno la stessa root e quindi hanno lo stesso nome, quindi l'arco
    viene eliminato direttamente.
    Altro caso in chi si vuole inserire lo stesso arco è quando il nodo ha un arco con la root,
    ma in questo caso i due nodi hanno lo stesso nome e quindi l'arco viene eliminato.
    """
    i = 0
    while i < len(node.listaArchi):
        edge = node.listaArchi[i]
        nodo1, nodo2 = edge.endpoints()
        if nodo1.element != nodo2.element:
            root.listaArchi.append( edge )
            i = i + 1
        else:
            if node.posizione == nodo1.posizione or node.posizione == nodo2.posizione:  # se tra le due estermità non è presente il node in input alla funzione non serve cancellare l'arco dalla sua lista
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


    peso_albero=0
    successor_next=Array( "i", g.vertex_count(), lock=False )
    modificato=Array("i",g.vertex_count(),lock=False)

    jobs_min=JoinableQueue()
    minimo_paralelo(parent,jobs_min,modificato)

    jobs_jump=JoinableQueue()
    jump_parallelo(parent,successor_next,jobs_jump)

    jobs_copy=JoinableQueue()
    copy_parallelo(parent,successor_next,jobs_copy)



    lista_nodi_boruvka=grafoB.vertices()

    if g.iscon():

        while len( lista_nodi ) > 1:
            for i in range(len(modificato)):
                modificato[i]=-1

            lista_divisa = dividi_gruppi( lista_nodi, 8 )
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

                add_jobs(jobs_jump,lista_divisa)
                jobs_jump.join()
                for x,y in zip(parent,successor_next):
                    if x!=y:
                        print(x,y,flush=True)
                        change=True
                        break

                if change==False:
                    break

                add_jobs(jobs_copy,lista_divisa)
                jobs_copy.join()



            for j in range( len( parent ) ):
                nodo = lista_nodi_originale[j]
                nodo.root = lista_nodi_originale[parent[nodo.element()]]
                nodo.setElement( nodo.root.element() )  # il nodo prende il nome della root
                for edge in nodo.listaArchi:

                    n1,n2=edge.endpoints()
                    n1.root=parent[n1.element]
                    n2.root=parent[n2.element]
                    n1.setElement(parent[n1.element])
                    n2.setElement(parent[n2.element])

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

