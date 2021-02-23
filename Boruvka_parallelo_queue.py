from multiprocessing import Process,Array,JoinableQueue,Queue,Manager
from Graph2 import Graph
from time import time
from random import randint
from Boruvka_sequenziale import Boruvka_seq







def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()
    g2=Graph()

    dict_edge={}



    n=10000
    for i in range(n):
        v0=g.insert_vertex(i)
        g2.insert_vertex(i)
        v0.connessioni=Array("i",n,lock=False)
        v0.pesi_condivisi=Array("i",n,lock=False)


    nodi=g.vertices()
    nodi2=g2.vertices()


    for i in range(0,len(nodi)):
        while True:
            peso = randint( 1, 100000000 )
            if g.peso_unico(peso):
                if i+1==len(nodi):
                    e=g.insert_edge( nodi[i], nodi[0], peso )
                    dict_edge[e.element()]=e
                    nodi[i].pesi_condivisi[nodi[0].element()]=peso
                    nodi[0].pesi_condivisi[nodi[i].element()]=peso

                    nodi[i].connessioni[len(nodi[i].listaArchi)-1]=nodi[0].element()
                    nodi[0].connessioni[len(nodi[0].listaArchi)-1]=nodi[i].element()


                    g2.insert_edge( nodi2[i], nodi2[0], peso )
                    break
                else:
                    e=g.insert_edge( nodi[i], nodi[i+1], peso )
                    dict_edge[e.element()]=e
                    nodi[i].pesi_condivisi[nodi[i+1].element()]=peso
                    nodi[i+1].pesi_condivisi[nodi[i].element()]=peso
                    nodi[i].connessioni[len(nodi[i].listaArchi)-1]=nodi[i+1].element()
                    nodi[i+1].connessioni[len(nodi[i+1].listaArchi)-1]=nodi[i].element()

                    g2.insert_edge( nodi2[i], nodi2[i+1], peso )
                    break

    for node in nodi:
        if len(node.listaArchi)<n:
            node.connessioni[len(node.listaArchi)]=-1



    for node in nodi:
        i=0

        print("node",node,flush=True)
        while i<99:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint(0, n-1)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                e=g.insert_edge( node, nodi[nodo2], peso )
                dict_edge[e.element()]=e
                g2.insert_edge( nodi2[node.element()], nodi2[nodo2], peso )

                node.pesi_condivisi[nodi[nodo2].element()]=peso
                nodi[nodo2].pesi_condivisi[node.element()]=peso
                node.connessioni[len(node.listaArchi)-1]=nodi[nodo2].element()
                nodi[nodo2].connessioni[len( nodi[nodo2].listaArchi)-1]=node.element()
                i=i+1


    lista_pesi_condivisi=[]
    lista_connessioni_condivise=[]

    for node in nodi:
        if len(node.listaArchi)<n:
            node.connessioni[len(node.listaArchi)]=-1

        lista_pesi_condivisi.append(node.pesi_condivisi)
        lista_connessioni_condivise.append(node.connessioni)





    return g,g2,lista_pesi_condivisi,lista_connessioni_condivise,dict_edge



def add_jobs_merge(dict_merge,jobs):
    for x,y in dict_merge.items():
        jobs.put((x,y))



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


def add_jobs(jobs, lista,sentinella=None):
    if sentinella==4:
        for x,y in lista.items():
            jobs.put((sentinella,(x,y)))
    else:
        for group in lista:
            jobs.put((sentinella,group))



def worker_minimo(jobs,parent,successor_next,lista_pesi_condivisi,lista_connessioni,result):
    while True:

        sentinella,group=jobs.get()



        if sentinella==0:
            t=time()
            result_list=[]
            for node in group:
                pesi=lista_pesi_condivisi[node]
                connessioni=lista_connessioni[node]

                minEdge = None

                for con in connessioni:
                    if con==-1:
                        break
                    if con!=node:
                        peso=pesi[con]

                        if minEdge == None or minEdge > peso:
                            opposto=con
                            minEdge = peso




                if minEdge!=None:
                    result_list.append((node,minEdge))
                    parent[node]=opposto

            result.put(result_list)


            jobs.task_done()

        if sentinella==1:
            lista_return=[None for i in range(len(parent))]
            for node in group:

                lista_return[node]= parent[parent[node]]

            result.put(lista_return)

            jobs.task_done()

        if sentinella==2:
            for node in group:
                parent[node]=successor_next[node]
            jobs.task_done()

        if sentinella==3:
            for node in group:
                pesi=lista_pesi_condivisi[node]
                connessioni=lista_connessioni[node]
                for i,con in enumerate(connessioni):
                    if con==-1:
                        break
                    if pesi[con]<pesi[parent[con]]:
                        pesi[parent[con]]=pesi[con]
                    elif pesi[parent[con]]==0:
                        pesi[parent[con]]=pesi[con]
                    connessioni[i]=parent[con]
            jobs.task_done()

        if sentinella==4:
            root,lista_merge=group
            pesi_root=lista_pesi_condivisi[root]
            dict_conn={}

            for i,conn2 in enumerate(lista_connessioni[root]):
                if conn2==-1:
                    dict_conn[conn2]=i
                    break
                dict_conn[conn2]=i
            for node in lista_merge:




                pesi_node=lista_pesi_condivisi[node]

                for conn in lista_connessioni[node]:

                    if conn!=root:
                        if conn==-1:
                            break
                        if pesi_root[conn]==0:

                            pesi_root[conn]=pesi_node[conn]
                            lista_connessioni[root][dict_conn[-1]]=conn

                            if dict_conn[-1]+1<len(lista_connessioni):
                                lista_connessioni[root][(dict_conn[-1]+1)]=-1
                                pos=dict_conn[-1]+1
                                dict_conn[-1]=pos
                        else:
                            if pesi_root[conn]>pesi_node[conn]:
                                pesi_root[conn]=pesi_node[conn]

                connessioni_root=lista_connessioni[root]

                dict_conn={}
                for i,conn in enumerate(connessioni_root):
                    if conn==-1:
                        dict_conn[-1]=i
                        break

                i=0
                while i <len(connessioni_root):

                    conn=connessioni_root[i]
                    if conn==-1:
                        break
                    if conn==root:

                        connessioni_root[i]=connessioni_root[dict_conn[-1]-1]
                        connessioni_root[dict_conn[-1]-1]=-1
                        pos_ultima=dict_conn[-1]-1
                        dict_conn[-1]=pos_ultima
                    else:
                        if dict_conn.get(conn) is not None:

                            connessioni_root[i]=connessioni_root[(dict_conn[-1]-1)]


                            connessioni_root[dict_conn[-1]-1]=-1
                            pos_ultima=dict_conn[-1]-1
                            dict_conn[-1]=pos_ultima
                        else:
                            dict_conn[conn]=True
                            i=i+1

            jobs.task_done()






def cerca_minimo_parallelo(n_pro, jobs,parent,successor_next,lista_pesi_condivisi,lista_connessioni,result):
    processes=[None for _ in range(n_pro)]
    for i in range( n_pro ):
        processes[i] = Process( target=worker_minimo, args=(jobs, parent,successor_next,lista_pesi_condivisi,lista_connessioni,result) )
        processes[i].daemon = True
        processes[i].start()
    return processes



def minimo_paralelo(parent,successor_next,lista_pesi_condivisi,lista_connessioni,jobs,result):

    return cerca_minimo_parallelo( 8,jobs,parent,successor_next,lista_pesi_condivisi,lista_connessioni,result)









def Boruvka_parallel_queue(g,lista_pesi_condivisi,lista_connessioni,dict_edge):
    grafoB = Graph()


    lista_nodi = g.vertices()

    for node in lista_nodi:
        grafoB.insert_vertex( node.element() )

    peso_albero=0
    parent = Array( "i", g.vertex_count(), lock=False )
    successor_next =Array( "i",g.vertex_count(), lock=False )


    result=Queue()
    jobs_min=JoinableQueue()
    processes_minimo=minimo_paralelo(parent,successor_next,lista_pesi_condivisi,lista_connessioni,jobs_min,result)

    lista_nodi_boruvka=grafoB.vertices()









    while len( lista_nodi ) > 1:
        lista_divisa_interi=dividi_gruppi(lista_nodi,8)
        add_jobs(jobs_min,lista_divisa_interi,0)
        jobs_min.join()


        while result.qsize()>0:
            lista_result=result.get()
            for node,edge_r in lista_result:
                edge=dict_edge[edge_r]
                n1,n2=edge.endpoints_posizione()
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

        while True:
            bool = True

            add_jobs(jobs_min,lista_divisa_interi,1)
            jobs_min.join()

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

            add_jobs(jobs_min,lista_divisa_interi,2)
            jobs_min.join()


        for node in lista_nodi:
            node.root =parent[node.element()]
            node.setElement( node.root )



        dict_merge={}


        for node in lista_nodi:
            if node.root==node.posizione:
                dict_merge[node.root]=[]


        add_jobs(jobs_min,lista_divisa_interi,3)
        jobs_min.join()

        lista=[x for x in lista_nodi]
        lista_nodi=[]
        for node in lista:
            if node.posizione!=node.root:
                dict_merge[node.root].append(node.posizione)
            else:
                lista_nodi.append(node)

        if len(lista_nodi)<=1:
            break

        add_jobs(jobs_min,dict_merge,4)
        jobs_min.join()

    for pr in processes_minimo:
        pr.terminate()





    return (grafoB,peso_albero)


if __name__ == '__main__':
    g,g_seq,lista_pesi_condivisi,lista_connessioni,dict_edge=creaRandom()
    print("costr")

    grafoB,peso_albero=Boruvka_parallel_queue(g,lista_pesi_condivisi,lista_connessioni,dict_edge)
    print(grafoB.edges_count())












