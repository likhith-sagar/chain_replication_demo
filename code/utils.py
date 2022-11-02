import uuid
import socket

def generateId():
    return uuid.uuid1().hex

def getFreeTcpPort():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    _, port = tcp.getsockname()
    tcp.close()
    return port