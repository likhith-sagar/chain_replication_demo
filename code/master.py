import rpyc
from rpyc.utils.server import ThreadedServer
import pickle

myPort = 5400

'''
nodes = [id1, id2, id3, ...]
nodeDetails: {
    id1: [address, prevNodeId, nextNodeId],
    .
    .
}
clients = {
    id1: address   
}

- Address here will be port number only, cuz IP will always be Localhost
- Ids can be port number here, but just to have it generalized, lets have a dedicated Id (random ID)
'''

nodes = []
nodeDetails = {}
clients = {}

class MasterService(rpyc.Service):
    
    def exposed_registerClient(self, id, address):
        '''
        Save client details
        return head and tail addresses
        '''
        clients[id] = address
        print(f'Registered client {id} @ {address}')
        head = None
        tail = None
        if len(nodes):
            head = nodeDetails[nodes[0]][0]
            tail = nodeDetails[nodes[-1]][0]
        return (head, tail)
    
    def exposed_registerNode(self, id, address):
        '''
        Save node details
        return predecessor and successor addresses
        '''
        prevNodeId = nodes[-1] if len(nodes) > 0 else None
        nodeDetails[id] = [address, prevNodeId, None]
        nodes.append(id)

        if prevNodeId != None:
            #inform the new node about its prev node
            try:
                con = rpyc.connect('localhost', address)
                con.root.updatePredecessor(nodeDetails[prevNodeId][0])
                con.close()
            except Exception as e:
                print(e)
                print(f'Failed to connect Node {id} @ {address}')
            
            #inform the prev node about its new successor
            try:
                con = rpyc.connect('localhost', nodeDetails[prevNodeId][0])
                con.root.updateSuccessor(address)
                con.close()
            except Exception as e:
                print(e)
                print(f'Failed to connect {prevNodeId} @ {nodeDetails[prevNodeId][0]}')
        
        print(f'Registered client {id} @ {address}')
        
if __name__ == "__main__":
    server = ThreadedServer(MasterService, port=myPort)
    print(f'Master started @ {myPort}')
    server.start()