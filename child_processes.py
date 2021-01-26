from mpi4py import MPI
comm_main_process = MPI.Comm.Get_parent()
comm_process=MPI.COMM_WORLD
size = comm_process.Get_size()
rank = comm_process.Get_rank()


def merge(lista_merge):
    for node in lista_merge:
        if node.posizione!=node.root:
            root=dict_root[node.root]
            for edge in node.listaArchi:
                nodo1, nodo2 = edge.endpoints()
                if nodo1 != nodo2:
                    root.listaArchi.append( edge )

        else:
            i=0
            while i < len( node.listaArchi ):
                edge = node.listaArchi[i]
                n1, n2 = edge.endpoints()
                if n1 == n2:
                    node.listaArchi.pop( i )
                else:
                    i = i + 1

def find_min_edges(lista_nodi):
    risultati=[]
    for node in lista_nodi:
        minEdge=None
        for edge in node.listaArchi:
            if minEdge is None or minEdge.element()>edge.element():
                minEdge=edge

        if minEdge is not None:
            risultati.append((minEdge,node.element()))
    return risultati

def change_name_node(lista_nodi):
    for node in lista_nodi:
        node.setElement(parent[node.element()])
        node.root=parent[node.element()]
        for edge in node.listaArchi:
            n1,n2=edge.endpoints()
            edge.setElement(parent[n1],parent[n2])

def build_list_inv(lista_nodi,mapp,lista_return_merge,lista_merge):
    for node in lista_nodi:
        if mapp[node.root]!=rank:
            lista_inv[mapp[node.root]].append(node)
        else:
            if node.posizione==node.root:
                lista_return_merge.append(node)
                dict_root[node.root]=node
            lista_merge.append(node)

if __name__=="__main__":
    lista_nodi,mapp=comm_main_process.recv(source=0)

    comm_main_process.send(find_min_edges(lista_nodi),dest=0)

    parent=comm_main_process.recv(source=0)

    change_name_node(lista_nodi)

    lista_merge=[]
    dict_root={}
    lista_inv=[[] for x in range(size)]
    lista_return_merge=[]

    build_list_inv(lista_nodi,mapp,lista_return_merge,lista_merge)

    for i in range(size):
        if i!=rank:
                comm_process.send(lista_inv[i],dest=i)
                lista_recv=comm_process.recv(source=i)
                for node in lista_recv:
                    lista_merge.append(node)


    merge(lista_merge)

    comm_main_process.send(lista_return_merge,dest=0)

    comm_process.Disconnect()

    comm_main_process.Disconnect()
