import socket
import random

# common variables which i can't figure out how to import
fromClientMask = 0b1000
fromWorkerMask = 0b100
fromWorkerDeclarationMask = 0b101
fromIngressMask = 0b10
bufferSize = 1024

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
        #worker = random.choice(workers) # select worker
        bytesToSend = 0b11.to_bytes(1, 'big') + fromIngressMask.to_bytes(1, 'big') + (len(clients)-1).to_bytes(1, 'big') + str.encode("Hello UDP worker")
        UDPServerSocket.sendto(bytesToSend, workers[0])

    # if message is from worker
    elif message[1] & fromWorkerMask == fromWorkerMask:
        if message[1] & fromWorkerDeclarationMask == fromWorkerDeclarationMask:
            workers.append(address)
            msg = "Worker declared: {}".format(message)
            IP = "Worker IP address: {}".format(address)
            print(msg)
            print(IP)
            continue

        # message is from worker but not declaration
        msg = "Message from worker: {}".format(message)
        IP = "Worker IP address: {}".format(address)
        print(msg)
        print(IP)
        client = message[2]
        if client > len(clients):
            print("ERROR INVALID CLIENT")
            continue
        # Sending a reply to the client
        UDPServerSocket.sendto(str.encode("Successful return to UDP client"), clients[client])
