import rpyc
from rpyc.utils.server import ThreadedServer
import pickle

myPort = 5400

'''
nodes = [id1, id2, id3, ...]
nodeDetails: {
    id1: {
        'address': --,
        'prevNode': --,
        'nextNode': --
    },
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
    
    def exposed_registerClient(id, address):
        '''
        Save client details
        return head and tail addresses
        '''
        pass
    
    def exposed_registerNode(id, address):
        '''
        Save node details
        return predecessor and successor addresses
        '''
        pass

if __name__ == "__main__":
    server = ThreadedServer(MasterService, myPort)
    server.start()