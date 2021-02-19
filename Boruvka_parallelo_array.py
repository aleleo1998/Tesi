from multiprocessing import Process,Array,JoinableQueue,Queue
from Graph2 import Graph
from time import time
from random import randint
from Boruvka_sequenziale import Boruvka_seq







def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    g2=Graph()





    n=5000

    for i in range(n):
        v0=g.insert_vertex(i)
        g2.insert_vertex(i)
        v0.archi_condivisi=Array("i",n-1,lock=False)

    nodi=g.vertices()
    nodi2=g2.vertices()


    for i in range(0,len(nodi)):
        while True:
            peso = randint( 1, 1000000000 )
            if g.peso_unico(peso):
                if i+1==len(nodi):
                    g.insert_edge( nodi[i], nodi[0], peso )
                    g2.insert_edge( nodi2[i], nodi2[0], peso )
                    break
                else:
                    g.insert_edge( nodi[i], nodi[i+1], peso )
                    g2.insert_edge( nodi2[i], nodi2[i+1], peso )
                    break

    for node in nodi:
        for i,edge in enumerate(node.incident_edges()):
            node.archi_condivisi[i]=edge.element()



    lista_archi_condivisi=[]
    for node in nodi:
        i=0
        while i<1000:
            peso = randint( 1, 1000000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0, n-1)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                g.insert_edge( node, nodi[nodo2], peso )
                g2.insert_edge( nodi2[node.element()], nodi2[nodo2], peso )
                node.archi_condivisi[len(node.listaArchi)-1]=peso
                nodi[nodo2].archi_condivisi[len(nodi[nodo2].listaArchi)-1]=peso
                i=i+1




        lista_archi_condivisi.append(node.archi_condivisi)


    return g,g2,lista_archi_condivisi







def dividi_gruppi(lista_nodi, n):
    i = 0

    num = int( len( lista_nodi ) / n ) + 1
    cont = 0
    lista_return_interi = [[] for _ in range( n )] # lista di ritorno

    while i < len( lista_nodi ):
        for _ in range( int( num ) ):
            if cont == n:
                """
                il contatore cont riparte da zero in modo tale da avere una distribuzione uniforme
                dei nodi sulle varie liste
                """
                cont = 0
            #lista_return_prova[cont].append(lista_nodi[i])
            #lista_return[cont].append(lista_nodi[i])
            lista_return_interi[cont].append(lista_nodi[i].element())
            i = i + 1
            cont = cont + 1

            if i == len( lista_nodi ): break
    return lista_return_interi


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


def add_jobs(jobs, lista,sentinella):
    for group in lista:
        jobs.put((sentinella,group))








def worker_minimo(jobs,parent,successor_next, lista_archi_condivisi,result):
    while True:

        sentinella,group=jobs.get()

        if sentinella==0:
            t=time()

            for node in group:

                minEdge = None

                for i,edge in enumerate(lista_archi_condivisi[node]):
                    if edge==0:
                        break

                    if minEdge == None or minEdge > edge:
                        minEdge = edge


                #lista_archi_condivisi[node][min]=0

                if minEdge!=None:
                    result[node]=minEdge

            #result.put(return_result)


            jobs.task_done()

        if sentinella==1:
            for node in group:
                successor_next[node]=parent[parent[node]]


            jobs.task_done()

        if sentinella==2:
            for node in group:
                parent[node]=successor_next[node]
            jobs.task_done()




def cerca_minimo_parallelo(n_pro, jobs,parent,successor_next, lista_archi_condivisi,result):
    for i in range( n_pro ):
        process = Process( target=worker_minimo, args=(jobs, parent,successor_next,lista_archi_condivisi,result) )
        process.daemon = True
        process.start()


def minimo_paralelo(parent,successor_next,lista_archi_condivisi,jobs,result):

    cerca_minimo_parallelo( 8,jobs,parent,successor_next,lista_archi_condivisi,result)




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

def Boruvka_parallel_array(g,lista_archi_condivisi):
    grafoB = Graph()

    dict_edge={}







    lista_nodi = g.vertices()
    for node in lista_nodi:
        for edge in node.incident_edges():
            dict_edge[edge.element()]=edge



    lista_nodi_originale=g.vertices()
    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )

    peso_albero=0
    parent = Array( "i", g.vertex_count(), lock=False )
    successor_next =Array( "i",g.vertex_count(), lock=False )


    result=Array("i",g.vertex_count(),lock=False)
    jobs_min=JoinableQueue()
    minimo_paralelo(parent,successor_next,lista_archi_condivisi,jobs_min,result)
    lista_nodi_boruvka=grafoB.vertices()



    t1 = time()
    while len( lista_nodi ) > 1:
        lista_divisa_interi=dividi_gruppi(lista_nodi,8)
        add_jobs(jobs_min,lista_divisa_interi,0)
        jobs_min.join()




        for node in lista_nodi:
            edge=dict_edge[result[node.element()]]


            n1,n2=edge.endpoints()
            if node.element()==n1:
                parent[n1]=n2
            else:
                parent[n2]=n1
            e=grafoB.insert_edge(lista_nodi_boruvka[n1],lista_nodi_boruvka[n2]
                                 ,edge.element())


            if e is not None:
                peso_albero+=edge.element()













        for node in lista_nodi:
            i=node.element()
            parent_opposto = parent[parent[i]]
            if i == parent_opposto:
                if i < parent[i]:
                    parent[i] = i
                else:
                    parent[parent[i]] = parent[i]







        tempo_pointer=time()
        while True:
            bool = True

            add_jobs(jobs_min,lista_divisa_interi,1)
            jobs_min.join()



            for x, y in zip( parent, successor_next ):
                if x != y:
                    bool = False
                    break

            if bool:
                break

            add_jobs(jobs_min,lista_divisa_interi,2)
            jobs_min.join()

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



        t2=time()
        dict_edge={}


        for node in lista_nodi:

            for i,edge in enumerate(node.incident_edges()):
                node.archi_condivisi[i]=edge.element()
                dict_edge[edge.element()]=edge
            i=i+1
            if i<len(node.archi_condivisi):
                node.archi_condivisi[i]=0

        #print(time()-t2)









    print( "\nTEMPO DI ESECUZIONE", time() - t1,"COSTO ALBERO",peso_albero,flush=True)






    return (grafoB,peso_albero)






if __name__ == '__main__':
    g,g_seq,lista_archi_condivisi=creaRandom()
    t_seq=time()
    mst,cost=Boruvka_seq(g_seq)
    print(time()-t_seq)





    print(g.edges_count(),flush=True)


    grafoB,peso_albero=Boruvka_parallel_array(g,lista_archi_condivisi)
    print(grafoB.edges_count())












    #Controllo se ogni arco restuito dall'algoritmo di Prim sia presente nell'albero costruito.