
from collections import defaultdict
from time import time
class Graph_seq:
    # These are the four small functions used in main Boruvkas function
    # It does union of two sets of x and y with the help of rank
    def union(self, parent, rank, x, y):
        xroot = self.find(parent, x)
        yroot = self.find(parent, y)
        if rank[xroot] < rank[yroot]:
            parent[xroot] = yroot
        elif rank[xroot] > rank[yroot]:
            parent[yroot] = xroot
        else :
            parent[yroot] = xroot #Make one as root and increment.
            rank[xroot] += 1

    def __init__(self,vertices):
        self.V= vertices
        self.graph = [] # default dictionary


    # add an edge to the graph
    def addEdge(self,u,v,w):
        self.graph.append([u,v,w])

        # find set of an element i
    def find(self, parent, i):
        if parent[i] == i:
            return i
        return self.find(parent, parent[i])


    #***********************************************************************
    #constructing MST
    def boruvkaMST(self):
        parent = [];
        rank = [];
        cheapest =[]
        numTrees = self.V
        MSTweight = 0
        for node in range(self.V):
            parent.append(node)
            rank.append(0)
            cheapest =[-1] * self.V

            # Keep combining components (or sets) until all
        # compnentes are not combined into single MST

        while numTrees > 1:
            for i in range(len(self.graph)):

                u,v,w = self.graph[i]
                set1 = self.find(parent, u)
                set2 = self.find(parent ,v)


                if set1 != set2:

                    if cheapest[set1] == -1 or cheapest[set1][2] > w :
                        cheapest[set1] = [u,v,w]

                    if cheapest[set2] == -1 or cheapest[set2][2] > w :
                        cheapest[set2] = [u,v,w]

                        # Consider the above picked cheapest edges and add them to MST
            for node in range(self.V):
                if cheapest[node] != -1:
                    u,v,w = cheapest[node]
                    set1 = self.find(parent, u)
                    set2 = self.find(parent ,v)

                    if set1 != set2 :
                        MSTweight += w
                        self.union(parent, rank, set1, set2)
                        #print ("Edge %d-%d has weight %d is included in MST" % (u,v,w))
                        numTrees = numTrees - 1


            cheapest =[-1] * self.V

        return MSTweight


        #print ("Weight of MST is %d" % MSTweight)












