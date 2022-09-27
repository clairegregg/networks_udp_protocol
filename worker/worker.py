import socket

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

ingressAddressPort = ("", 20001)

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = (baseHeaderBuild(numberOfHeaderBytesBase, (declarationMask|fromWorkerMask), noClientSelected)
    + noFileSegment.to_bytes(1, 'big')
    + str.encode("Worker declaring itself to ingress"))
UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)

print("Worker UDP server up and listening")

# Listen for incoming messages
while True:
    bytesAddressPair = UDPWorkerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]
    msgFromIngress = "Message from ingress: {}".format(message)
    ingressIP = "Ingress IP address: {}".format(address)

    print(msgFromIngress)
    print(ingressIP)

    headerLength = message[0]
    fileName = message[numberOfHeaderBytesBase:headerLength] # Gives file name which is after base header and before any other explanatory message
    # Sending a reply to ingress
    bytesToSend = None

    with open(fileName.decode(), "rb") as f:
        bytes_read = f.read()
        filePart = 0
        startRead = 0
        while True:
            endRead = startRead + bufferSize-headerLength

            # If this is the final segment
            if endRead > len(bytes_read):
                endRead = len(bytes_read)-1
                # Ensure notFinalSegment bit not set to represent that this is the final segment
                bytesToSend = (baseHeaderBuild(headerLength, fromWorkerMask,
                message[2]))
                send = bytesToSend + filePart.to_bytes(1, 'big') + fileName + bytes_read[startRead:endRead]
                UDPWorkerSocket.sendto(send, ingressAddressPort)
                break

            # Set notFinalSegment bit to represent that there are more segments of this file to come
            bytesToSend = (baseHeaderBuild(headerLength, fromWorkerMask|notFinalSegmentMask,
            message[2]))
            send = bytesToSend + filePart.to_bytes(1, 'big') + fileName + bytes_read[startRead:endRead]
            UDPWorkerSocket.sendto(send, ingressAddressPort)
            startRead = endRead
            filePart += 1
