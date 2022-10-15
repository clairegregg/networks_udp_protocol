import socket
import protocol_lib

def get_parts_received(message):
    numBytesPartsReceived = message[protocol_lib.bytesOfReceivedPartsIndex]
    headerLength = message[protocol_lib.headerLengthIndex]
    partsReceivedBytes = int.from_bytes(message[headerLength:headerLength+numBytesPartsReceived], "big")
    partsReceived = []

    for i in reversed(range((numBytesPartsReceived * 8)-1)):
        if partsReceivedBytes & 0b1 == 0b1:
            partsReceived.append(i)
        partsReceivedBytes  = partsReceivedBytes >> 1

    return partsReceived

def send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived):
    print("sending file, length ", len(currentFile))
    for partIndex in range(len(currentFile)):
        print("Sending part ", partIndex)
        # Do not send a segment which has been received again
        if partIndex in partsReceived:
            continue
        if partIndex == len(currentFile)-1:
            actionByte = protocol_lib.fromWorkerMask|protocol_lib.notFinalSegmentMask
        else:
            actionByte = protocol_lib.fromWorkerMask

        bytesToSend = (
            protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase+len(currentFileName), actionByte, client)
            + partIndex.to_bytes(1, 'big')
            + str.encode(currentFileName)
            + currentFile[partIndex]
        )

        UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)

def get_file(message, currentFileName, currentFile):
    headerLength = message[protocol_lib.headerLengthIndex]
    fileName = message[protocol_lib.numberOfHeaderBytesRequest:headerLength]
    if fileName == currentFileName:
        return currentFile
    else:
        currentFile = []
        with open(fileName.decode(), "rb") as f:
            bytes_read = f.read()
            filePart = 0
            startRead = 0
            while True:
                print("Reading part {}".format(filePart))
                endRead = startRead + protocol_lib.bufferSize-headerLength

                # If this is the final segment
                if endRead > len(bytes_read):
                    endRead = len(bytes_read)-1
                    currentFile.append(bytes_read[startRead:endRead])
                    break
                currentFile.append(bytes_read[startRead:endRead])
                startRead = endRead
                filePart += 1
        print("File has this many packets ", len(currentFile))
        return currentFile


ingressAddressPort = ("", protocol_lib.ingressPort)
currentFileName = ""
currentFile = []

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = (protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase, (protocol_lib.declarationMask|protocol_lib.fromWorkerMask), protocol_lib.noClientSelected)
    + protocol_lib.noFileSegment.to_bytes(1, 'big')
    + str.encode("Worker declaring itself to ingress"))
UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)

print("Worker UDP server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPWorkerSocket.recvfrom(protocol_lib.bufferSize)
    print("Received??????????")
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]
    msgFromIngress = "Message from ingress: {}".format(message)
    ingressIP = "Ingress IP address: {}".format(address)
    print(msgFromIngress)
    print(ingressIP)
    currentFile = get_file(message, currentFileName, currentFile)
    print("got file")
    partsReceived = get_parts_received(message)
    print("got parts received")
    client = message[protocol_lib.clientIndex]
    print("got client")
    send_file(currentFileName, currentFile, client, UDPWorkerSocket, partsReceived)
