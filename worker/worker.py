import socket
import protocol_lib
import time

# Global variables
# Number of files sent at one time
chunk_size = 3

# Time waited between sending chunks of files
wait_time = 0.005

# Address port to request files from
ingressAddressPort = ("", protocol_lib.ingressPort)

# Global tracking of the current file, and its contents
currentFileName = ""
currentFile = []

# Read which parts a client has received from its header.
def get_parts_received(message):
    # Get values from header
    numBytesPartsReceived = protocol_lib.getReceivedPartsBytes(message)
    headerLength = message[protocol_lib.headerLengthIndex]
    partsReceivedBytes = int.from_bytes(message[headerLength:headerLength+numBytesPartsReceived], "big")
    partsReceived = []

    # Loop through partsReceivedBytes and add parts which have been received to the list
    for i in reversed(range((numBytesPartsReceived * 8))):
        if partsReceivedBytes & 0b1 == 0b1:
            partsReceived.append(i)
        partsReceivedBytes  = partsReceivedBytes >> 1
    return partsReceived

# Send a file to client
def send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived):
    numFilesSentInChunk = 0

    # Loop through the file
    for partIndex in range(len(currentFile)):
        # Leave gap after sending certain number of files to have greater chance of them being received
        if numFilesSentInChunk >= chunk_size:
            time.sleep(wait_time)
            numFilesSentInChunk = 0

        # Do not send a segment which has been received again
        if partIndex in partsReceived:
            continue

        # If it is the last segment
        if partIndex == len(currentFile)-1:
            control = protocol_lib.fromWorkerMask
        else:
            control = protocol_lib.fromWorkerMask|protocol_lib.notFinalSegmentMask
        
        # Create the header and send the file
        bytesToSend = (
            protocol_lib.baseHeaderBuild(
                protocol_lib.numberOfHeaderBytesBase+len(currentFileName), 
                control, 
                client, 
                partIndex
            )
            + str.encode(currentFileName)
            + currentFile[partIndex]
        )
        UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)
        numFilesSentInChunk += 1

# Read the file from memory
def get_file(message, currentFileName, currentFile):
    # Get the file name
    headerLength = message[protocol_lib.headerLengthIndex]
    fileName = message[protocol_lib.numberOfHeaderBytesRequest:headerLength].decode()

    # If this file has been read most recently, return it
    if fileName == currentFileName:
        return currentFileName, currentFile
    # Otherwise, read the file from memory and return it
    else:
        currentFile = []
        with open(fileName, "rb") as f:
            bytes_read = f.read()
            filePart = 0
            startRead = 0
            while True:
                endRead = startRead + protocol_lib.bufferSize-headerLength

                # If this is the final segment
                if endRead > len(bytes_read):
                    endRead = len(bytes_read)-1
                    currentFile.append(bytes_read[startRead:endRead])
                    break
                currentFile.append(bytes_read[startRead:endRead])
                startRead = endRead
                filePart += 1
        return (fileName, currentFile)

##################################
###### Main part of program ######
##################################
# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = (protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase, (protocol_lib.declarationMask|protocol_lib.fromWorkerMask), protocol_lib.noClientSelected, protocol_lib.noFileSegment)
    + str.encode("Worker declaring itself to ingress"))
UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)

print("Worker UDP server up and listening")

# Listen for incoming messages
while True:
    message = UDPWorkerSocket.recvfrom(protocol_lib.bufferSize)[0]

    currentFileName, currentFile = get_file(message, currentFileName, currentFile)
    partsReceived = get_parts_received(message)
    client = message[protocol_lib.clientIndex]
    send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived)
