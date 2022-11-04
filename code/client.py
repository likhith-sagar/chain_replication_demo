import rpyc
import utils

masterPort = 5400
myId = utils.generateId()

con = rpyc.connect('localhost', masterPort)
head, tail = con.root.registerClient(myId, 5000)
con.close()

while True:
    inp = int(input("Enter: "))
    if inp == 1:
        key, val = input().split(',')
        con = rpyc.connect('localhost', head)
        con.root.execute('update', key, val)
        con.close()
    elif inp == 2:
        key = input()
        con = rpyc.connect('localhost', head)
        value = con.root.execute('read', key)
        con.close()
        print(f'Value: {value}')
    else:
        break
