import socket
import protocol_lib
import time

chunk_size = 3
wait_time = 0.005

def get_parts_received(message):
    numBytesPartsReceived = message[protocol_lib.bytesOfReceivedPartsIndex]
    headerLength = message[protocol_lib.headerLengthIndex]
    partsReceivedBytes = int.from_bytes(message[headerLength:headerLength+numBytesPartsReceived], "big")
    partsReceived = []

    for i in reversed(range((numBytesPartsReceived * 8))):
        if partsReceivedBytes & 0b1 == 0b1:
            partsReceived.append(i)
        partsReceivedBytes  = partsReceivedBytes >> 1
    return partsReceived

def send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived):
    numFilesSentInChunk = 0
    for partIndex in range(len(currentFile)):
        # Leave gap after sending certain number of files to have greater chance of them being received
        if numFilesSentInChunk >= chunk_size:
            time.sleep(wait_time)
            numFilesSentInChunk = 0

        # Do not send a segment which has been received again
        if partIndex in partsReceived:
            continue
        if partIndex == len(currentFile)-1:
            actionByte = protocol_lib.fromWorkerMask
        else:
            actionByte = protocol_lib.fromWorkerMask|protocol_lib.notFinalSegmentMask
        bytesToSend = (
            protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase+len(currentFileName), actionByte, client, partIndex)
            + str.encode(currentFileName)
            + currentFile[partIndex]
        )
        UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)
        numFilesSentInChunk += 1

def get_file(message, currentFileName, currentFile):
    headerLength = message[protocol_lib.headerLengthIndex]
    
    headerAndReceivedLength = protocol_lib.numberOfHeaderBytesRequest + (message[protocol_lib.bytesOfReceivedPartsIndex])
    fileName = message[protocol_lib.numberOfHeaderBytesRequest:headerLength].decode()
    if fileName == currentFileName:
        return currentFileName, currentFile
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


ingressAddressPort = ("", protocol_lib.ingressPort)
currentFileName = ""
currentFile = []

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = (protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase, (protocol_lib.declarationMask|protocol_lib.fromWorkerMask), protocol_lib.noClientSelected, protocol_lib.noFileSegment)
    + str.encode("Worker declaring itself to ingress"))
UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)

print("Worker UDP server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPWorkerSocket.recvfrom(protocol_lib.bufferSize)
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]
    currentFileName, currentFile = get_file(message, currentFileName, currentFile)
    partsReceived = get_parts_received(message)
    client = message[protocol_lib.clientIndex]
    send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived)
