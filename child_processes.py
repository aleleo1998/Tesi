from mpi4py import MPI
from time import time
comm_main_process = MPI.Comm.Get_parent()
comm_process=MPI.COMM_WORLD
size = comm_process.Get_size()
rank = comm_process.Get_rank()



def merge(lista_merge):
    for node in lista_merge:
        if node.posizione!=node.root:
            root=dict_root[node.root]
            edges=node.listaArchi.copy()
            for edge in edges.values():
                nodo1, nodo2 = edge.endpoints()
                if nodo1 != nodo2:
                    if nodo1==root.element():
                        root.add_arco_root(nodo2,edge)
                    else:
                        root.add_arco_root(nodo1,edge)



def find_min_edges(lista_nodi):
    risultati=[]
    for node in lista_nodi:
        minEdge=None
        for edge in node.incident_edges():
            if minEdge is None or minEdge.element()>edge.element():
                minEdge=edge

        if minEdge is not None:
            risultati.append((minEdge,node.element()))
    return risultati

def change_name_node(lista_nodi):
    for node in lista_nodi:
        node.setElement(parent[node.element()])
        node.root=parent[node.element()]
        for edge in node.incident_edges():
            n1,n2=edge.endpoints()
            edge.setElement(parent[n1],parent[n2])

def build_list_inv(lista_nodi,map,lista_return_merge,lista_merge):
    for node in lista_nodi:
        if map[node.root]!=rank:
            lista_inv[map[node.root]].append(node.posizione)
        else:
            if node.posizione==node.root:
                lista_return_merge.append(node)
                dict_root[node.root]=node
            lista_merge.append(node)

def delete_multi_edges(lista_return_merge):
    for node in lista_return_merge:
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
if __name__=="__main__":
    while True:
        task=comm_main_process.recv(source=0)
        if task==False:
            break
        lista_nodi,map,processi_attivi=task



        comm_main_process.send(find_min_edges(lista_nodi),dest=0)

        while True:
            task2=comm_main_process.recv(source=0)

            if task2==False:
                break
            parent=task2
            successor=[-1 for el in parent]
            for node in lista_nodi:
                successor[node.element()]=parent[parent[node.element()]]
            comm_main_process.send(successor,dest=0)



        parent=comm_main_process.recv(source=0)
        change_name_node(lista_nodi)

        lista_merge=[]
        dict_root={}
        tempo_merge=time()
        lista_inv=[[] for x in range(size)]
        lista_return_merge=[]

        build_list_inv(lista_nodi,map,lista_return_merge,lista_merge)



        comm_main_process.send(lista_inv,dest=0)
                   # lista_recv=comm_process.recv(source=i)
                    #for node in lista_recv:
                      #  lista_merge.append(node)


        lista_recv=comm_main_process.recv(source=0)

        for node in lista_recv:
            node.setElement(parent[node.element()])
            node.root=parent[node.element()]
            lista_merge.append(node)

        for node in lista_merge:
            for edge in node.incident_edges():
                n1,n2=edge.endpoints()
                edge.setElement(parent[n1],parent[n2])





        merge(lista_merge)

        delete_multi_edges(lista_return_merge)

        comm_main_process.send(lista_return_merge,dest=0)

    comm_process.Disconnect()

    comm_main_process.Disconnect()
