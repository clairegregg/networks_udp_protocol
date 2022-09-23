import socket
import random

# common variables which i can't figure out how to import
numberOfHeaderBytesBase = 0b11
noClientSelected = 0b0
fromClientMask = 0b1000
fromWorkerMask = 0b100
fromWorkerDeclarationMask = 0b101
fromIngressMask = 0b10
bufferSize = 65507
def baseHeaderBuild(length, actionSelector, client):
    return length.to_bytes(1, 'big') + actionSelector.to_bytes(1, 'big') + client.to_bytes(1, 'big')

localIP = ""
localPort = 20001
workers = []
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
    if message[1] & fromClientMask == fromClientMask:
        msg = "Message from client: {}".format(message)
        IP = "Client IP address: {}".format(address)
        print(msg)
        print(IP)
        clients.append(address)

        no_workers = len(workers) < 1
        if no_workers:
            print("no_workers")
            continue

        worker = random.choice(workers) # select worker in a better way
        bytesToSend = (baseHeaderBuild(message[0], fromIngressMask, len(clients)-1)
            + message[numberOfHeaderBytesBase:message[0]] # Gives file name which is after base header and before any other explanatory message
            + str.encode("Ingress passing along file request"))

        UDPServerSocket.sendto(bytesToSend, worker)

    # if message is from worker
    elif message[1] & fromWorkerMask == fromWorkerMask:

        # If message is a declaration from worker
        if message[1] & fromWorkerDeclarationMask == fromWorkerDeclarationMask:
            workers.append(address)
            msg = "Worker declared: {}".format(message)
            IP = "Worker IP address: {}".format(address)
            print(msg)
            print(IP)
            continue

        # message is from worker but is not declaration
        msg = "Message from worker: {}".format(message)
        IP = "Worker IP address: {}".format(address)
        print(msg)
        print(IP)
        client = message[2]
        if client > len(clients):
            print("ERROR INVALID CLIENT")
            continue

        bytesToSend = (baseHeaderBuild(message[0], fromIngressMask, noClientSelected)
            + message[numberOfHeaderBytesBase:message[0]] # Gives file name which is after base header and before any other explanatory message
            + message[message[0]:])
        # Sending a reply to the client
        UDPServerSocket.sendto(bytesToSend, clients[client])
