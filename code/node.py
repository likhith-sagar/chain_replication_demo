import rpyc
from rpyc.utils.server import ThreadedServer
import utils
from threading import Thread
from time import sleep
import pickle

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
pending = [
    [reqId, key, value],
    .
    .
]
'''

keyValues = {

}
pending = []
lastProcessed = -1
nextReqId = 0
lastAcknowledged = -1


peers = [None, None] #[prevAddress, nextAddress]

def register(id, port, masterPort):
    sleep(2)
    try:
        con = rpyc.connect('localhost', masterPort)
        #might have to use pickle here to send data across properly
        con.root.registerNode(id, port)
        con.close()
    except Exception as e:
        print(e)
        print("Failed to connect to master")
        exit()

class NodeService(rpyc.Service):

    def exposed_updateSuccessor(self, address):
        peers[1] = address
        print(f'Successor updated to {address}')

        #if no successor, I'm the tail
        if address == None:
            if len(pending):
                lastPendingReq = pending.pop()
                lastAcknowledged = lastPendingReq[0]
                try:
                    con = rpyc.connect('localhost', peers[0])
                    con.root.acknowledge(lastAcknowledged)
                    con.close()
                except:
                    print(f'Failed to acknowledge {lastAcknowledged}')
            return

        #having ref, because pending list gets altered by acknowledge method
        pendingReqs = pending
        if len(pendingReqs):
            try:
                con = rpyc.connect('localhost', peers[1])
                for req in pendingReqs:
                    con.root.request(pickle.dumps(req))
                con.close()
                print('Successfully forwarded pending requests')
            except Exception as e:
                print(e)
                print(f'Failed to connect to {address}')
            #to be continued
    
    def exposed_updatePredecessor(self, address, copy=False):
        global keyValues, lastProcessed, nextReqId, lastAcknowledged
        peers[0] = address
        print(f'Predecessor updated to {address}')
        if copy and address != None:
            try:
                con = rpyc.connect('localhost', address)
                data = con.root.copyData()
                keyValsBin, lastProcessed, nextReqId, lastAcknowledged = pickle.loads(data)
                keyValues = pickle.loads(keyValsBin)
                con.close()
                print("Data copied successfully")
            except Exception as e:
                print(e)
                print("Failed to copy Data")
            
    def exposed_copyData(self):
        keyValsBin = pickle.dumps(keyValues)
        return pickle.dumps([keyValsBin, lastProcessed, nextReqId, lastAcknowledged])

    def exposed_execute(self, queryType, key, value=None):
        '''
        This will be called only on head and tails
        update key value storage
        pass the query to next node (figuring out how (should clarify about logs first))
        '''
        global temp, con
        if queryType == 'read':
            if key  in keyValues:
                return keyValues[key]
            else:
                return None

        if queryType == 'update':
            request = [nextReqId, key, value]
            self.processRequest(request)

            #If I'm both head and the tail
            if peers[1] == None:
                return

            try:
                con = rpyc.connect('localhost', peers[1])
                async_req = rpyc.async_(con.root.request)
                async_req(pickle.dumps(request))
                # con.root.request(request)
                # con.close()
                print(f'Forwarded request {request[0]}')
            except Exception as e:
                print(e)
                print(f'Failed to forward request {request[0]}')
        #to be completed
    
    def exposed_acknowledge(self, reqId):
        global pending, lastAcknowledged
        if reqId <= lastAcknowledged:
            return #ignore
        
        i = 0
        while i < len(pending):
            if pending[i][0] == reqId:
                break
            i+=1
        pending = pending[i+1:]
        lastAcknowledged = reqId
        print(f"Acknowledge received {reqId}")
        
        #if no predecessor, I'm the head
        if peers[0] == None:
            return
        try:
            con = rpyc.connect('localhost', peers[0])
            async_ack = rpyc.async_(con.root.acknowledge)
            async_ack(reqId)
            con.close()
            print(f'Acknowledge passed {reqId}')
        except:
            print(f'Failed to pass acknowledge {reqId}')

    #helper
    def processRequest(self, request):
        global lastProcessed, nextReqId
        _, key, value = request
        if value == None:
            if key in keyValues:
                keyValues.pop(key)
        else:
            keyValues[key] = value
        lastProcessed = request[0]
        nextReqId = request[0]+1
        #If I'm the tail, there's no pending req
        if peers[1] != None:
            pending.append(request)
        print(f'Processed request {request[0]}')
    
    def exposed_request(self, request):
        '''
        request: [redId, key, value]
        value == None => delete the key
        '''
        global lastAcknowledged
        # for i in range(100):
        #     print(f"{i} req called")
        #     print(request, lastAcknowledged)
        request = pickle.loads(request)
        if request[0] <= lastAcknowledged:
            #request already processed and acknowledged
            try:
                con = rpyc.connect('localhost', peers[0])
                async_ack = rpyc.async_(con.root.acknowledge)
                async_ack(lastAcknowledged)
                con.close()
                print(f'Acknowledged {lastAcknowledged}')
            except Exception as e:
                print(e)
                print(f'Failed to acknowledge {lastAcknowledged}')
            return
        if request[0] <= lastProcessed:
            #request already processed but not acknowledged
            return #no necessary actions required
        
        self.processRequest(request)
        #if no successor, I'm the tail
        if peers[1] == None:
            try:
                con = rpyc.connect('localhost', peers[0])
                async_ack = rpyc.async_(con.root.acknowledge)
                async_ack(request[0])
                con.close()
                print(f'Acknowledged {request[0]}')
            except Exception as e:
                print(e)
                print(f"Failed to acknowledge {request[0]}")
            lastAcknowledged = request[0]
        else:
            try:
                con = rpyc.connect('localhost', peers[1])
                async_req = rpyc.async_(con.root.request)
                async_req(pickle.dumps(request))
                con.close()
                print(f'Forwarded request {request[0]}')
            except Exception as e:
                print(e)
                print(f"Failed to forward request {request[0]}")
    
    def exposed_ping(self):
        return myId
       
if __name__ == "__main__":
    server = ThreadedServer(NodeService, port=myPort)
    print(f'Node {myId} started @ {myPort}')
    t = Thread(target=register, args=[myId, myPort, masterPort], daemon=True)
    t.start()
    server.start()
 