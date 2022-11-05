import rpyc
from rpyc.utils.server import ThreadedServer
from threading import Thread
import utils
import pickle

masterPort = 5400
myPort = utils.getFreeTcpPort()
myId = utils.generateId()

con = rpyc.connect('localhost', masterPort)
head, tail = con.root.registerClient(myId, myPort)
con.close()

def interface():
    def update(key, value):
        try:
            con = rpyc.connect('localhost', head)
            con.root.execute('update', key, value)
            con.close()
            print('Successful')
        except:
            print(f'Failed connecting to head @ {head}')
        
    def read(key):
        try:
            con = rpyc.connect('localhost', tail)
            value = con.root.execute('read', key)
            con.close()
            print(f'Value: {value}')
        except:
            print(f'Failed connecting to tail @ {tail}')
    
    while True:
        print('\n1. Add/Update\n2. Delete\n3. Read\n4. About\n0. Exit')
        choice = int(input())
        if choice == 1:
            key = input("Enter key: ")
            value = input("Enter value: ")
            update(key, value)
        elif choice == 2:
            key = input("Enter key: ")
            update(key, None)
        elif choice == 3:
            key = input('Enter key: ')
            read(key)
        elif choice == 4:
            print(f'Client {myId} @ {myPort}')
            print(f'Head: {head}\nTail: {tail}')
        elif choice == 0:
            print('Ctrl+C to stop client server')
            break
        else:
            continue


class ClientService(rpyc.Service):
    def exposed_updateHead(self, headAddress):
        global head
        head = headAddress
        # print(f'head updated to {headAddress}')
    
    def exposed_updateTail(self, tailAddress):
        global tail
        tail = tailAddress
        # print(f'tail updated to {tailAddress}')
    
if __name__ == "__main__":
    server = ThreadedServer(ClientService, port=myPort)
    print(f'Client {myId} started @ {myPort}')
    print(f'Head: {head}\nTail: {tail}')
    t = Thread(target=interface)
    t.start()
    server.start()


