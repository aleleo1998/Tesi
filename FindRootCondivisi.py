import multiprocessing
from Graph import Graph
import collections
import concurrent.futures
import time
import random

"""
FUNZIONI PARALLELE PER LA CREAZIONE DEL GRAFO RANDOM
"""
def worker_inserimento(l,g):
    pass

def inserisci_archi(lista_nodi,g):
    features=set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for l in get_jobs(lista_nodi):
            features.add(executor.submit(worker_inserimento,l,g))


def creaGrafo():
    """
    CREAZIONE DEL GRAFO DI PROVA
    IL PRIMO GRAFO è COPIATO DAL GRAFO PRESENTE SU WIKIPEDIA CHE MOSTRA L'ESECUZIONE
    DELL'ALGORITMO DI BORUVKA.
    """

    g = Graph( False )
    """
    v0=g.insert_vertex(0)
    v1=g.insert_vertex(1)
    v2=g.insert_vertex(2)
    v3=g.insert_vertex(3)
    v4=g.insert_vertex(4)
    v5=g.insert_vertex(5)

    v6 = g.insert_vertex(6)
    v7 = g.insert_vertex(7)
    v8 = g.insert_vertex(8)
    v9 = g.insert_vertex(9)
    v10 = g.insert_vertex(10)
    v11= g.insert_vertex(11)



    g.insert_edge(v0,v1,13)
    g.insert_edge(v0,v2,6)
    g.insert_edge(v1,v2,7)
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
    """
    #CREAZIONE RANDOM DEL GRAFO
    for i in range(1000):
        v0=g.insert_vertex(i)
    print(g.vertex_count())

    for node in g.vertices():
        for _ in range(3):
            peso = random.randint( 1, 10000000000000000000000000000000000000000000000)
            #NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            #LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = random.randint( 0, 999)
            if nodo2 != node.element() and g.peso_unico(peso):

                e=g.insert_edge( node.element(), nodo2, peso )
                if e is None:
                    print("non inserisco")
                else:
                    print("Inserisco")
            else:
                print( "non inserisco" )

    return g

def dividi_gruppi(lista_nodi,n):
    """

    :param lista_nodi: lista dei nodi che riceve
    :param n: in quante liste i nodi devono essere divisi
    :return:
    """
    i=0
    num=int(len(lista_nodi)/n)
    if num==0:
        num=1
    cont=0
    lista_return = [[] for _ in range(n)] # lista di ritorno
    while i < len( lista_nodi ):
        for _ in range(int(num)):
            if cont==n:
                """
                il contatore cont riparte da zero in modo tale da avere una distribuzione uniforme
                dei nodi sulle varie liste
                """
                cont=0
            lista_return[cont].append(lista_nodi[i])
            i=i+1
            cont=cont+1

            if i==len(lista_nodi):break
    return lista_return

def get_jobs(lista):
    for l in lista:
        yield l

def jump_worker(parent,successor_next,lista_nodi):
    """
    prima funzione paralle dell'algoritmo pointer_jumping
    """
    for node in lista_nodi:
        successor_next[node.element()]=parent[parent[node.element()]]
def jump(lista_divisa,parent,successor_next):
    futures=set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for lista_nodi in get_jobs(lista_divisa):
            futures.add(executor.submit(jump_worker,parent,successor_next,lista_nodi))


def copia_worker(lista_nodi,successor,successor_next):
    """
    seconda funzione parallale del pointer_jumping
    :param lista_nodi:
    :param successor_next:
    :param successor:
    :return:
    """
    for node in lista_nodi:
        successor[node.element()] = successor_next[node.element()]
def copia_successor(lista_divisa,successor,successor_next):
    futures=set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for lista_nodi in get_jobs(lista_divisa):
            futures.add(executor.submit(copia_worker(lista_nodi,successor,successor_next)))

def worker_minimo(parent,lista_nodi):
    risultati=[]
    """
    funzione parallela per ricerca gli archi di costo minimo
    """
    for node in lista_nodi:
        min=-1
        minEdge=None
        for edge in node.listaArchi:
            if min==-1 or min>edge.element():
                minEdge=edge
                min=edge.element()

        try:
            n1,n2=minEdge.endpoints()
        except:
            print( node.listaArchi )
        """
        I controlli di seguito sono necesserai perchè nella listaArchi della root
        possono essere presenti archi che non hanno come una delle due estremità la
        root stessa
        """
        if n1.element()==node.element():
            parent[node.element()]=n2.element()
        else:
            parent[node.element()] = n1.element()
        risultati.append(minEdge)
    return risultati

def cerca_minimo_parallelo(lista_divisa,parent,grafoB):
    futures = set()

    with concurrent.futures.ThreadPoolExecutor( max_workers=8 ) as executor:
            for lista_nodi in get_jobs(lista_divisa):
                futures.add( executor.submit( worker_minimo,parent,lista_nodi) )
            wait_for( futures,grafoB )

def wait_for(futures,grafoB):
    for future in concurrent.futures.as_completed(futures):
        for edge in future.result():
            """
            Inserimento degli archi trovati nel grafo che si sta costruendo
            """
            nodo1, nodo2 = edge.endpoints()
            e = grafoB.insert_edge( grafoB.vertices()[nodo1.posizione], grafoB.vertices()[nodo2.posizione],
                                    edge.element() )
            """
            La funzione insert_edge ritornerà None quando un arco è già stato inserito
            """
            if e is not None:
                """
                Per la stampa si usa sempre nodo.posizione che rappresenta
                la posizione del nodo all'interno della lista dei nodi del grafo a cui appartiene.
                Siccome si stanno usando gli interi la posizione rappresenterà anche il nome originario del nodo 
                (ovviamente il nodo cambia nome durante l'esecuzione).
                """
                print( "inserisco arco:({},{},{})".format( nodo1.posizione, nodo2.posizione, edge.element() ) )





def delete_edges(node):
    """
    Questa funzione viene invocata solo per le root che si sono trovate.
    All'interno della listaArchi della root possono esserci archi con gli estremi che hanno lo stesso nome,
    in questo caso vuol dire che i due nodi apparetengo alla stessa componente e quindi,l'arco, non deve essere considerato
    nell'iterazione successiva
    """
    i=0
    while i<len(node.listaArchi):
        edge=node.listaArchi[i]
        n1, n2 = edge.endpoints()
        if n1.element() == n2.element():
            node.listaArchi.remove( edge )
        else:
            i=i+1 # si incremente solo in caso in cui non si cancella un edge

def merge(node,root):
    try:
        """
        In questa funzione si inserisco gli archi di un nodo nella listaArchi della propria root.
        Gli archi con i nodi che hanno lo stesso nome non vengono considerati.
        Gli archi che non devono essere considerati vengono eliminati dalla listaArchi di tutti i due nodi
        così da non considerarli in una ultereriore iterazione.
        Alle volte capita che qualche arco viene già eliminato nella lista di uno dei due nodi.
        """
        lista=node.listaArchi.copy()
        for edge in lista:
            nodo1, nodo2 = edge.endpoints()
            if nodo1.element()!=nodo2.element():
                root.listaArchi.append(edge)
            else:
                nodo1.listaArchi.remove(edge)
                nodo2.listaArchi.remove(edge)

    except:
        print("arco già cancellato")

"""
def worker_merge(lista_divisa,lista_nodi):
    risultati={node:[] for node in lista_divisa }
    for node in lista_divisa:
        if node.root!=node:

            for edge in node.listaArchi:
                n1,n2=edge.endpoints()
                if n1.element()!=n2.element():
                    risultati[node].append(edge)

            lista_nodi.remove(node)
        else:
            for edge in node.listaArchi:
                n1,n2=edge.endpoints()
                if n1.element()==n2.element():
                    risultati[node].append(edge)

    return risultati
def merge_paralleo(lista_divisa,lista_nodi):
    futures=set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for l in get_jobs(lista_divisa):
            futures.add(executor.submit(worker_merge,l,lista_nodi))
        wait_for_merge(futures)

def wait_for_merge(futures):
    for f in concurrent.futures.as_completed(futures):
        for node,lista in f.result().items():
            if node!=node.root:
                for edge in lista:
                    node.root.listaArchi.append(edge)
            else:
                for edge in lista:
                    node.listaArchi.remove(edge)
"""

if __name__=="__main__":

    g=creaGrafo() #grafo originale
    grafoB=Graph() #grado che si costruisce
    for node in g.vertices():
        grafoB.insert_vertex(node.element()) # si inseriscono un numero di vertici pari a quello del grafo originale e con gli stessi nomi.
    lista_nodi=g.vertices() #la lista_nodi inzialmente conterra tutti i nodi del grafo
    parent=multiprocessing.Array("i",g.vertex_count(),lock=False) #array condivisi
    successor_next=multiprocessing.Array("i",g.vertex_count(),lock=False)
    t1 = time.time()
    iterazione=0
    print(g.iscon())
    if g.iscon():
        while len(lista_nodi)>1:
            lista_divisa=dividi_gruppi(lista_nodi,8) #si divide la lista dei nodi , scelto 8 perchè sarà
                                                     # il numero dei processi che si userà ogni volta
            cerca_minimo_parallelo(lista_divisa,parent,grafoB)


            """
            Nel caso in cui due nodi abbiano scelto lo stesso arco, ognuno sarà
            il parent dell'altro.
            In questo caso si scopre il nodo con l'identificativo (nome) più piccolo e  
            il suo parent sarà se stesso (diventa radice).
            """
            for i in range(len(parent)):
                parent_opposto=parent[parent[i]]
                if i==parent_opposto:
                    if i<parent_opposto:
                        parent[i]=i
                    else:
                        parent[parent_opposto]=parent_opposto

            """
            Algoritmo di wikipedia (pointer_jumping)
            """
            while True:

                bool=True
                jump(lista_divisa,parent,successor_next)
                for x,y in zip(parent,successor_next):

                    if x!=y:
                        print( x, y )
                        bool = False
                        break
                if bool:
                    break;
                copia_successor(lista_divisa,parent,successor_next)


            """
            Aggiornamento dei nodi del grafo originale.
            """
            for j in range(len(parent)):
                nodo=g.vertices()[j]
                nodo.root = g.vertices()[parent[nodo.element()]]
                nodo.setElement(nodo.root.element()) #il nodo prende il nome della root


            i=0

            while i<len(lista_nodi):
                node=lista_nodi[i]
                if node!=node.root:
                    merge(node,node.root)
                    lista_nodi.remove(node)
                else:
                    i=i+1

            #merge_paralleo(lista_divisa,lista_nodi)

            """
            Nella lista_nodi resteranno solo i nodi che sono root.
            """



            """
            Se la lista_nodi conterrà soltano un nodo significherà che avremo solo una 
            root.
            """
            if len(lista_nodi)>1:
               for node in lista_nodi:
                    delete_edges(node)

            iterazione+=1



    print("\nTEMPO DI ESECUZIONE",time.time()-t1)
    if grafoB.iscon():
        print("\nGrafo costruito correttamente")













