import socket
import queue

# common variables which i can't figure out how to import
numberOfHeaderBytesBase = 0b100
noClientSelected = 0b0
noFileSegment = 0b0
fromClientMask = 0b1000
fromWorkerMask = 0b100
declarationMask = 0b1
fromIngressMask = 0b10
notFinalSegmentMask = 0b10000
headerLengthIndex = 0
actionSelectorIndex = 1
clientIndex = 2
partOfFileIndex = 3
bufferSize = 65507
def baseHeaderBuild(length, actionSelector, client):
    return length.to_bytes(1, 'big') + actionSelector.to_bytes(1, 'big') + client.to_bytes(1, 'big')

localIP = ""
localPort = 20001
workers = queue.Queue(0)
clients = []

# Create a UDP socket
UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind socket to IP and port
UDPServerSocket.bind((localIP, localPort))

print("UDP ingress server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]

    # if message is from client
    if message[actionSelectorIndex] & fromClientMask == fromClientMask:
        msg = "Message from client: {}".format(message)
        IP = "Client IP address: {}".format(address)
        print(msg)
        print(IP)
        clients.append(address)

        worker = workers.get(True, 0) # If there is no worker currently available, block until there is
        bytesToSend = (baseHeaderBuild(message[headerLengthIndex], fromIngressMask, len(clients)-1)
            + message[partOfFileIndex].to_bytes(1, 'big')
            + message[numberOfHeaderBytesBase:message[headerLengthIndex]] # Gives file name which is after base header and before any other explanatory message
            + str.encode("Ingress passing along file request"))

        UDPServerSocket.sendto(bytesToSend, worker)

    # if message is from worker
    elif message[actionSelectorIndex] & fromWorkerMask == fromWorkerMask:

        # If message is a declaration from worker
        if message[1] & declarationMask == declarationMask:
            msg = "Worker declared: {}".format(message)
            IP = "Worker IP address: {}".format(address)
            print(msg)
            print(IP)
            workers.put(address)
            continue

        # message is from worker but is not declaration
        msg = "Message from worker received"
        IP = "Worker IP address: {}".format(address)
        print(msg)
        print(IP)

        client = message[clientIndex]
        if client > len(clients):
            print("ERROR INVALID CLIENT")
            continue

        bytesToSend = None
        # If it is the final segment, that means the worker is ready
        if message[actionSelectorIndex] & notFinalSegmentMask != notFinalSegmentMask:
            workers.put(address)
            bytesToSend = (baseHeaderBuild(message[0], fromIngressMask, noClientSelected)
            + message[partOfFileIndex].to_bytes(1, 'big')
            + message[numberOfHeaderBytesBase:message[0]] # Gives file name which is after base header and before any other explanatory message
            + message[message[0]:])
        else:
            bytesToSend = (baseHeaderBuild(message[0], fromIngressMask|notFinalSegmentMask, noClientSelected)
                + message[partOfFileIndex].to_bytes(1, 'big')
                + message[numberOfHeaderBytesBase:message[0]] # Gives file name which is after base header and before any other explanatory message
                + message[message[0]:])
        # Sending a reply to the client
        UDPServerSocket.sendto(bytesToSend, clients[client])
