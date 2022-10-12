import socket
import queue
import protocol_lib
import multiprocessing

def deal_with_input(bytesAddressPair, workers, clients, lockWorkers, lockClients):
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]

    # if message is from client
    if message[protocol_lib.actionSelectorIndex] & protocol_lib.fromClientMask == protocol_lib.fromClientMask:
        msg = "Message from client: {}".format(message)
        IP = "Client IP address: {}".format(address)
        print(msg)
        print(IP)
        lockClients.acquire()
        clients.append(address)
        print("Client {} requesting file {}".format(len(clients), message[protocol_lib.numberOfHeaderBytesBase:message[protocol_lib.headerLengthIndex]]))
        lockClients.release()

        # TODO: Make this not block infinitely if the queue is empty - this should never happen but just in case
        lockWorkers.acquire()
        worker = workers.get(block=True, timeout=None) # If there is no worker currently available, block until there is
        lockWorkers.release()
        bytesToSend = (protocol_lib.baseHeaderBuild(message[protocol_lib.headerLengthIndex], protocol_lib.fromIngressMask, len(clients)-1)
            + message[protocol_lib.partOfFileIndex].to_bytes(1, 'big')
            + message[protocol_lib.numberOfHeaderBytesBase:message[protocol_lib.headerLengthIndex]] # Gives file name which is after base header and before any other explanatory message
            + str.encode("Ingress passing along file request"))

        UDPServerSocket.sendto(bytesToSend, worker)

    # if message is from worker
    elif message[protocol_lib.actionSelectorIndex] & protocol_lib.fromWorkerMask == protocol_lib.fromWorkerMask:

        # If message is a declaration from worker
        if message[protocol_lib.actionSelectorIndex] & protocol_lib.declarationMask == protocol_lib.declarationMask:
            msg = "Worker declared: {}".format(message)
            IP = "Worker IP address: {}".format(address)
            print(msg)
            print(IP)
            lockWorkers.acquire()
            workers.put(address)
            lockWorkers.release()
            return

        # message is from worker but is not declaration
        msg = "Message from worker received"
        IP = "Worker IP address: {}".format(address)
        print(msg)
        print(IP)

        client = message[protocol_lib.clientIndex]
        if client > len(clients):
            print("ERROR INVALID CLIENT")
            return

        bytesToSend = None
        print("Received part {}".format(message[protocol_lib.partOfFileIndex]))
        # If it is the final segment, that means the worker is ready
        if message[protocol_lib.actionSelectorIndex] & protocol_lib.notFinalSegmentMask != protocol_lib.notFinalSegmentMask:
            workers.put(address)
            bytesToSend = (protocol_lib.baseHeaderBuild(message[protocol_lib.headerLengthIndex], protocol_lib.fromIngressMask, protocol_lib.noClientSelected)
            + message[protocol_lib.partOfFileIndex].to_bytes(1, 'big')
            + message[protocol_lib.numberOfHeaderBytesBase:message[protocol_lib.headerLengthIndex]] # Gives file name which is after base header and before any other explanatory message
            + message[message[protocol_lib.headerLengthIndex]:])
        else:
            bytesToSend = (protocol_lib.baseHeaderBuild(message[protocol_lib.headerLengthIndex], protocol_lib.fromIngressMask|protocol_lib.notFinalSegmentMask, protocol_lib.noClientSelected)
                + message[protocol_lib.partOfFileIndex].to_bytes(1, 'big')
                + message[protocol_lib.numberOfHeaderBytesBase:message[protocol_lib.headerLengthIndex]] # Gives file name which is after base header and before any other explanatory message
                + message[message[protocol_lib.headerLengthIndex]:])
        # Sending a reply to the client
        UDPServerSocket.sendto(bytesToSend, clients[client])

# Main contents:

localIP = ""
localPort = protocol_lib.ingressPort
manager = multiprocessing.Manager()
workers = manager.Queue()
clients = manager.list()
lockWorkers = manager.Lock()
lockClients = manager.Lock()

# Create a UDP socket
UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind socket to IP and port
UDPServerSocket.bind((localIP, localPort))

print("UDP ingress server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPServerSocket.recvfrom(protocol_lib.bufferSize)
    print("Received message")

    process = multiprocessing.Process(target=deal_with_input,args=(bytesAddressPair,workers,clients,lockWorkers, lockClients))
    process.start()
    #process.join()
