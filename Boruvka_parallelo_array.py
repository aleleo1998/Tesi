from multiprocessing import Process,Array,JoinableQueue,Manager
from Graph2 import Graph
import concurrent.futures as cfutures
from time import time
from random import randint

def archi_worker(slice,parent,modificato,pesi,lista_nodi):
    result=[]
    for i in range(slice[0],slice[1]):
        node=lista_nodi[i]
        minEdge=None
        min=-1
        for edge in node.incident_edges():
            if min == -1 or min > edge.element():
                minEdge = edge
                min = edge.element()
        if minEdge is None:
            raise ValueError( "La lista del nodo in input è vuota" )
        n1, n2 = minEdge.endpoints()
        posizione1,posizione2=minEdge.endpoints_posizione()

        if n1 == node.element():
            parent[node.element()] = n2
            modificato[posizione1]=posizione2
            pesi[posizione1]=minEdge.element()
        elif n2 == node.element():
            modificato[posizione2]=posizione1
            parent[node.element()]=n1
            pesi[posizione2]=minEdge.element()
            #print(minEdge,flush=True)
        result.append(minEdge)
    return result

def archi_minimi(parent,lista_nodi,modificato,pesi,slice):
    futures=set()
    with cfutures.ThreadPoolExecutor(max_workers=8) as executor:
        for s in slice:
            futures.add(executor.submit(archi_worker,s,parent,modificato,pesi,lista_nodi))
        wait_for(futures)


def change_name_worker(slice,parent,lista_nodi):
    for i in range(slice[0],slice[1]):
        node=lista_nodi[i]
        node.root = parent[node.element()]
        node.setElement(node.root)
        for edge in node.incident_edges():
            n1,n2=edge.endpoints()
            edge.setElement(parent[n1],parent[n2])
    return None

def change_name(parent,lista_nodi,slice):
    futures=set()
    with cfutures.ThreadPoolExecutor(max_workers=8) as executor:
        for s in slice:
            futures.add(executor.submit(change_name_worker,s,parent,lista_nodi))
        wait_for(futures)

def wait_for(futures):
    for f in cfutures.as_completed(futures):
        pass#print(f.result(),flush=True)


def jump_worker(parent,successor_next,jobs):
    while True:
        group=jobs.get()
        for node in group:
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
        group=jobs.get()
        for node in group:
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
        g.insert_vertex(i)

    nodi=g.vertices()


    for i in range(1,len(nodi)):
        while True:
            peso = randint( 1, 100000000 )
            if g.peso_unico(peso):
                g.insert_edge( nodi[i-1], nodi[i], peso )
                break



    for node in nodi:
        i=0
        while i<999:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, 9999)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                g.insert_edge( node, nodi[nodo2], peso )
                i=i+1


    return g







def dividi_gruppi(lista_nodi, n):
    i = 0

    num = int( len( lista_nodi ) / n ) + 1
    cont = 0
    lista_return_interi = [[] for _ in range( n )] # lista di ritorno
    lista_return_prova = [[] for _ in range( n )]

    while i < len( lista_nodi ):
        for _ in range( int( num ) ):
            if cont == n:
                """
                il contatore cont riparte da zero in modo tale da avere una distribuzione uniforme
                dei nodi sulle varie liste
                """
                cont = 0
            lista_return_prova[cont].append((lista_nodi[i].element(),lista_nodi[i].listaArchi.values()))
            #lista_return[cont].append(lista_nodi[i])
            lista_return_interi[cont].append(lista_nodi[i].element())
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return lista_return_interi,lista_return_prova


def crea_slice(lunghezza_lista,n_pro):
    lista_slice=[[] for _ in range(n_pro)]

    cont=0

    incremento=int(lunghezza_lista/n_pro)
    i=0
    while cont<lunghezza_lista:
        if i==n_pro-1:
            lista_slice[i]=(cont,cont+(lunghezza_lista-cont))
            break

        if cont+incremento>lunghezza_lista:
            lista_slice[i]=(cont,cont+(lunghezza_lista-cont))
        else:
            lista_slice[i]=(cont,cont+incremento)
        cont+=incremento
        i=i+1
    return lista_slice


def add_jobs(jobs, lista):
    for node in lista:
        jobs.put(node)


def add_jobs_pointer_jumping(jobs,lista):
    for node in lista:
        jobs.put(node.element())







def worker_minimo(jobs, parent,modificato,pesi):
    while True:
        group=jobs.get()
        for node,lista_archi in group:

            min = -1
            minEdge = None

            for edge in lista_archi:
                if min == -1 or min > edge.element():
                    minEdge = edge
                    min = edge.element()
            if minEdge is None:
                raise ValueError( "La lista del nodo in input è vuota" )
            n1, n2 = minEdge.endpoints()
            posizione1,posizione2=minEdge.endpoints_posizione()

            if n1 == node:
                parent[node] = n2
                modificato[posizione1]=posizione2
                pesi[posizione1]=minEdge.element()
            elif n2 == node:
                modificato[posizione2]=posizione1
                parent[node]=n1
                pesi[posizione2]=minEdge.element()


        jobs.task_done()


def cerca_minimo_parallelo(n_pro, jobs, parent,modificato,pesi):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,modificato,pesi) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,jobs,modificato,pesi):

    cerca_minimo_parallelo( 8,jobs, parent,modificato,pesi)




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

def Boruvka_parallel_array(g):
    grafoB = Graph()
    manager=Manager()






    lista_nodi = g.vertices()



    lista_nodi_originale=g.vertices()
    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )
    peso_albero=0
    parent = Array( "i", g.vertex_count(), lock=False )
    successor_next =Array( "i", g.vertex_count(), lock=False )
    modificato=Array("i",g.vertex_count(),lock=False)
    pesi=Array("i",g.vertex_count(),lock=False)


    jobs_min=JoinableQueue()
    minimo_paralelo(parent,jobs_min,modificato,pesi)


    jobs_jump=JoinableQueue()
    jump_parallelo(parent,successor_next,jobs_jump)

    jobs_copy=JoinableQueue()
    copy_parallelo(parent,successor_next,jobs_copy)




    lista_nodi_boruvka=grafoB.vertices()





    t1 = time()
    while len( lista_nodi ) > 1:



        lista_divisa_interi,lista_divisa= dividi_gruppi(lista_nodi,8)
        #print(len(ca.lista_archi_nodi))
        #slice=crea_slice(len(lista_nodi),8)
        add_jobs(jobs_min,lista_divisa)
        jobs_min.join()
        #archi_minimi(parent,lista_nodi,modificato,pesi,slice)





        #for j in range(len(modificato)):

            #if modificato[j]!=-1:
        for node in lista_nodi:
                j=node.element()
                e=grafoB.insert_edge(lista_nodi_boruvka[j],lista_nodi_boruvka[modificato[j]]
                                     ,pesi[j])
                if e is not None:
                    #print(e,flush=True)
                    peso_albero+=pesi[j]



        for i in range( len( parent ) ):
            parent_opposto = parent[parent[i]]
            if i == parent_opposto:
                if i < parent[i]:
                    parent[i] = i
                else:
                    parent[parent[i]] = parent[i]



        for node in lista_nodi:
            node.root = parent[node.element()]
            node.setElement(node.root)
            for edge in node.incident_edges():
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])










        i = 0
        while i < len( lista_nodi ):
            node = lista_nodi[i]
            if node.posizione != node.root:
                merge( node,lista_nodi_originale[node.root] )
                lista_nodi.pop(i)
            else:
                i = i + 1

        delete_multi_edges(lista_nodi)






    print( "\nTEMPO DI ESECUZIONE", time() - t1,"COSTO ALBERO",peso_albero,flush=True)






    return (grafoB,peso_albero)






if __name__ == '__main__':
    g=creaRandom()


    print(g.edges_count())

    grafoB,peso_albero=Boruvka_parallel_array(g)






    #Controllo se ogni arco restuito dall'algoritmo di Prim sia presente nell'albero costruito.