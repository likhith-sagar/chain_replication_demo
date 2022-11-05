import rpyc
from rpyc.utils.server import ThreadedServer
import pickle
from time import sleep
from threading import Thread

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

heartBeatInterval = 5

def handleFailure():
    global nodes
    while True:
        sleep(heartBeatInterval)
        if len(nodes) == 0:
            continue
        curHead, curTail = (nodes[0], nodes[-1])
        #check if nodes are active
        failedNodes = set()
        for nodeId in nodes:
            nodePort = nodeDetails[nodeId][0]
            try:
                con = rpyc.connect('localhost', nodePort)
                id = con.root.ping()
                con.close()
                if id != nodeId:
                    raise Exception
            except:
                #Expected Node failed.
                # Posibility of new node on same address, but we are checking for it too
                failedNodes.add(nodeId)
        
        if len(failedNodes) == 0:
            continue #no failed nodes to handle
        
        print('Nodes failed: ', failedNodes)
        nodes = list(filter(lambda x: x not in failedNodes, nodes))
        for nodeId in failedNodes:
            nodeDetails.pop(nodeId)

        newHead, newTail = (None, None)
        if len(nodes) != 0:
            changes = [] #[[nodeId, 'p/s']] p: predecessor, s: successor
            prevId = nodes[0] #head
            if nodeDetails[prevId][1] != None:
                nodeDetails[prevId][1] = None
                changes.append([prevId, 'p'])

            for i in range(1, len(nodes)):
                curId = nodes[i]
                if nodeDetails[prevId][2] != curId:
                    #successor of prevId node is changed
                    #so does the predecessor of curId node
                    nodeDetails[prevId][2] = curId
                    nodeDetails[curId][1] = prevId
                    changes.append([prevId, 's'])
                    changes.append([curId, 'p'])
                prevId = curId

            #prevId will now be tail
            if nodeDetails[prevId][2] != None:
                nodeDetails[prevId][2] = None
                changes.append([prevId, 's'])
            
            #inform changes
            for change in changes:
                nodeId, action = change
                nodePort, preId, sucId = nodeDetails[nodeId]
                if action == 's':
                    sucPort = nodeDetails[sucId][0] if sucId else None
                    try:
                        con = rpyc.connect('localhost', nodePort)
                        async_updateSuc = rpyc.async_(con.root.updateSuccessor)
                        async_updateSuc(sucPort)
                        con.close()
                    except:
                        print(f'Failed to update successor for {nodeId} @ {nodePort}')
                else: #action has to be p here
                    prePort = nodeDetails[preId][0] if preId else None
                    try:
                        con = rpyc.connect('localhost', nodePort)
                        async_updatePre = rpyc.async_(con.root.updatePredecessor)
                        async_updatePre(prePort)
                        con.close()
                    except:
                        print(f'Failed to update predecessor for {nodeId} @ {nodePort}')
            newHead, newTail = (nodes[0], nodes[-1])
            print('Updated chain')

        #if head or tail has changed, update the clients
        if curHead != newHead:
            headPort = nodeDetails[newHead][0] if newHead else None
            for client in set(clients):
                try:
                    con = rpyc.connect('localhost', clients[client])
                    async_updateHead = rpyc.async_(con.root.updateHead)
                    async_updateHead(headPort)
                    con.close()
                except:
                    clients.pop(client)
            print('Updated clients with new head')
        if curTail != newTail:
            tailPort = nodeDetails[newTail][0] if newTail else None
            for client in set(clients):
                try:
                    con = rpyc.connect('localhost', clients[client])
                    async_updateTail = rpyc.async_(con.root.updateTail)
                    async_updateTail(tailPort)
                    con.close()
                except:
                    clients.pop(client)
            print('Updated clients with new tail')



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
        if prevNodeId:
            nodeDetails[prevNodeId][2] = id

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
        
        print(f'Registered node {id} @ {address}')
        
if __name__ == "__main__":
    server = ThreadedServer(MasterService, port=myPort)
    print(f'Master started @ {myPort}')
    t = Thread(target=handleFailure, daemon=True)
    t.start()
    server.start()