import rpyc
from rpyc.utils.server import ThreadedServer
import pickle
import utils

masterPort = 5400
myId = utils.generateId()
myPort = utils.getFreeTcpPort()

'''
keyValues = {
    key1: val1,
    key2: val2,
    .
    .
}
logs = [
    'logId1 update key1 val1',
    'logId2 delete key1 val1',
    .
    .
]
- Not sure if logs are the ones that gets transfered between nodes
- Lets keep it optional for now, I'll look into papers and update
- We can have nonCommitedKeyValues to store non-commited key-vals, but lets keep it optional for now
'''

keyValues = {

}

peers = None #[prevAddress, nextAddress]
try:
    con = rpyc.connect('localhost', masterPort)
    #might have to use pickle here to send data across properly
    peers = con.root.registerNode(myId, myPort)
    con.close()
except:
    print("Failed to connect to master")
    exit()

class NodeService(rpyc.service):
    def exposed_updateSuccessor(address):
        peers[1] = address
        #to be continued
    
    def exposed_updatePredecessor(address):
        peers[0] = address
        #since we'll be adding new nodes to tail, this will not get used (for now)
    
    def exposed_query(queryType, key, value):
        '''
        This will be called only on head and tails
        update key value storage
        pass the query to next node (figuring out how (should clarify about logs first))
        '''
        if queryType == 'update':
            pass
        if queryType == 'delete':
            pass
        #to be completed