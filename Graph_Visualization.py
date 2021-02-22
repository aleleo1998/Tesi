
"""
def creaRandom():
    # CREAZIONE RANDOM DEL GRAFO

    g = Graph()

    for i in range(20):

        g.insert_vertex( i )

    nodi=g.vertices()

    for node in nodi:
        i=0
        while i<2:
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
            nodo2 = randint( 0,19)
            if nodo2 != node.element() and g.get_edge(node,nodi[nodo2]) is None and g.peso_unico(peso):

                e = g.insert_edge( node, nodi[nodo2], peso )
                if e is not None:
                    i+=1

    return g

def crea_random_bt_cycle():
    g=Graph()
    for i in range(10000):
        g.insert_vertex(i)

    lista_nodi=g.vertices()
    for i in range(1,len(lista_nodi)):

        while True:
            peso=randint(1,1000000)
            if g.peso_unico(peso):
                g.insert_edge(lista_nodi[i-1],lista_nodi[i],peso)
                break


    for node in lista_nodi:
        for _ in range(2):
            peso = randint( 1, 100000000 )
            # NUMERO MOLTO GRANDE PER AVERE QUASI LA CERTEZZA DI NON AVERE ARCHI CON LO STESSO PESO
            # LA FUNZIONE PER IL CONTROLLO è PRESENTE NELA CLASSE DEL GRAFO MA IMPIEGA MOLTO TEMPO
        nodo2 = randint( 0,9999)
        if nodo2 != node.element() and g.get_edge(node,lista_nodi[nodo2]) is None and g.peso_unico(peso):

            e = g.insert_edge( node, lista_nodi[nodo2], peso )


    return g





def boruvka():

    lista_nodi=g.vertices()
    peso_albero=0
    mst=Graph()
    for node in g.vertices():
        mst.insert_vertex(node.element())

    parent=[-1]*g.vertex_count()
    while len(lista_nodi)>1:

        for node in lista_nodi:
            node.isRoot=False

        for node in lista_nodi:
            minedge=None
            for edge in node.incident_edges():

                if minedge==None or minedge.element()>edge.element():
                    minedge=edge
                    n1,n2=edge.endpoints()
                    if n1==node.element():
                        parent[node.element()]=n2
                    else:
                        parent[node.element()]=n1

            if minedge is not None:
                n1,n2=minedge.endpoints_posizione()
                e=mst.insert_edge(mst.vertices()[n1],mst.vertices()[n2],minedge.element())
                if e is not None:
                    edges_in_mst.add(tuple(sorted((n1,n2))))
                    yield edges_in_mst
                    peso_albero+=minedge.element()



        for node in lista_nodi:
            node_parent=parent[node.element()]
            parent_parent=parent[parent[node.element()]]

            if node.element()==parent_parent:

                if node.element()<node_parent:
                    parent[node.element()]=node.element()

                else:
                    parent[node_parent]=node_parent





        findRoot(parent)

        for j in range( len( parent ) ):
            nodo = g.vertices()[j]
            nodo.root = g.vertices()[parent[nodo.element()]]
            nodo.setElement( nodo.root.element() )
            for edge in nodo.incident_edges():
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])

        i=0
        while i<len(lista_nodi):
            node=lista_nodi[i]
            if node.root!=node:
                merge(node,node.root)
                lista_nodi.pop(i)
            else:
                i=i+1
                delete_edges(node)



def update(mst_edges):
    ax.clear()
    nx.draw_networkx_nodes(graph, pos, node_size=30, ax=ax)

    nx.draw_networkx_edges(
        graph, pos, edgelist=all_edges-mst_edges, alpha=0.1,
        edge_color='g', width=1, ax=ax
    )
    nx.draw_networkx_edges(
        graph, pos, edgelist=mst_edges, alpha=1.0,
        edge_color='r', width=1, ax=ax
    )

def do_nothing():
    pass





graph = nx.Graph()
g=crea_random_bt_cycle()



all_edges = set()
for edge in g.edges():
    n1,n2=edge.endpoints()
    if n1<n2:
        graph.add_edge(n1,n2)
    else:
        graph.add_edge(n2,n1)


all_edges = set(
    tuple(sorted((n1, n2))) for n1, n2 in graph.edges()
)

pos = nx.random_layout(graph)

edges_in_mst = set()


fig, ax = plt.subplots(figsize=(10,5))

ani = animation.FuncAnimation(fig,update,init_func=do_nothing,frames=boruvka,interval=1)


plt.show()

print("NUMBER OF NODES",graph.number_of_nodes(),"\nNUMBER OF EDGES",graph.number_of_edges())
"""

import matplotlib.pyplot as plt
import pandas as pd

# Data
df=pd.DataFrame({'x':[0,500000,1000000,1500000,2000000,3000000,3500000,4000000,5000000],
                 'Utilizzo di MPI':   [0, 13.4, 22.59, 36.669, 45.05, 76.19, 85.6, 102.44,112.5],
                 'Sequenziale':       [0, 22.28, 26.53, 29.5, 49.6, 54.17, 58.89, 65.4, 71.5],
                 "Utilizzo mp.Array": [0, 6.20,  6.79, 8.07,8.95, 9.39,  9.92,11.8,13.61],
                 "Utilizzo queue"   : [0,6.30,7.2,8.88,9.01,9.42,10.02,12.5,14.2],
                 'Utilizzo mp.Queue': [0, 13.7, 21.84, 30.572, 41.45, 69.14, 84.9,102.5,114.5],})




# multiple line plot
#plt.plot( df['x'], df['Utilizzo mp.Array'], color='b', linewidth=2,label="mp.Array con strutture condivise")
plt.plot( df['x'], df['Utilizzo mp.Queue'], color='r', linewidth=2,label="mp.Queue senza strutture condivise")
plt.plot( df['x'], df['Utilizzo queue'], color='b', linewidth=2,label="mp.Queue con strutture condivise")

plt.plot( df['x'], df['Utilizzo di MPI'], color='g', linewidth=2, label="MPI")
plt.plot( df['x'], df['Sequenziale'], color='y', linewidth=2 ,label="sequenziale")
plt.title("Grafo con 10mila nodi")
plt.xlabel("Numero di archi")
plt.ylabel("Tempo in secondi")
plt.legend()

plt.show()





