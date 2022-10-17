import socket
import protocol_lib
import multiprocessing

# Actions if the message is from client
def message_from_client(message, address, workers, clients, workersInUse, lockWorkers, lockClients, lockWorkersInUse):
    # Get the clients index
    if address in clients:
        clientIndex = clients.index(address)
    else:
        lockClients.acquire()
        clients.append(address)
        lockClients.release()
        clientIndex = len(clients)-1

    # If the file has been successfully received, add worker back to main queue
    if message[protocol_lib.actionSelectorIndex] & protocol_lib.fileAckMask == protocol_lib.fileAckMask:
        lockWorkersInUse.acquire()
        worker = workersInUse.pop(clientIndex)
        lockWorkersInUse.release()

        lockWorkers.acquire()
        workers.put(worker)
        lockWorkers.release()
        return

    # Find the client's worker (or assign one)
    if clientIndex not in workersInUse:
        worker = workers.get(block=True, timeout=None) # If there is no worker currently available, block until there is
        lockWorkersInUse.acquire()
        workersInUse[clientIndex] = worker
        lockWorkersInUse.release()
    else:
        worker = workersInUse[clientIndex]

    # Pass on the message with the client index added for the worker.
    bytesToSend = message[0:protocol_lib.clientIndex] + clientIndex.to_bytes(1, 'big') + message[protocol_lib.clientIndex+1:]
    UDPServerSocket.sendto(bytesToSend, worker)

# Actions if the message is from a worker
def message_from_worker(message, address, workers, clients, lockWorkers):
    # If message is a declaration from worker
    if message[protocol_lib.actionSelectorIndex] & protocol_lib.declarationMask == protocol_lib.declarationMask:
        lockWorkers.acquire()
        workers.put(address)
        lockWorkers.release()
        return

    # Message is from worker but is not a declaration
    client = message[protocol_lib.clientIndex]
    # Sending the reply to the client,
    UDPServerSocket.sendto(message, clients[client])

# Deal with any received message
def deal_with_recv(bytesAddressPair, workers, clients, workersInUse, lockWorkers, lockClients, lockWorkersInUse):
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]

    # if message is from client
    if message[protocol_lib.actionSelectorIndex] & protocol_lib.fromClientMask == protocol_lib.fromClientMask:
        message_from_client(message, address, workers, clients, workersInUse, lockWorkers, lockClients, lockWorkersInUse)

    # if message is from worker
    elif message[protocol_lib.actionSelectorIndex] & protocol_lib.fromWorkerMask == protocol_lib.fromWorkerMask:
        message_from_worker(message, address, workers, clients, lockWorkers)


##################################
###### Main part of program ######
##################################
# Define the port
localIP = ""
localPort = protocol_lib.ingressPort
UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
UDPServerSocket.bind((localIP, localPort))

# Create shared memory locations for shared variables in multiprocessing, with locks for mutual exclusion
manager = multiprocessing.Manager()
workers = manager.Queue()
clients = manager.list()
workersInUse = manager.dict()
lockWorkers = manager.Lock()
lockClients = manager.Lock()
lockWorkersInUse = manager.Lock()

print("UDP ingress server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPServerSocket.recvfrom(protocol_lib.bufferSize)

    # Start a new process when a message is received to deal with it.
    process = multiprocessing.Process(target=deal_with_recv,args=(bytesAddressPair,workers,clients, workersInUse, lockWorkers, lockClients, lockWorkersInUse))
    process.start()
