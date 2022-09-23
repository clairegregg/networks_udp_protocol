import socket

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

ingressAddressPort = ("", 20001)

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = (baseHeaderBuild(numberOfHeaderBytesBase, fromWorkerDeclarationMask, noClientSelected)
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
    bytesToSend = (baseHeaderBuild(headerLength, fromWorkerMask, message[2])
        + fileName)
    with open(fileName.decode(), "rb") as f:
        bytes_read = f.read(bufferSize-headerLength)
    bytesToSend += bytes_read

    UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)
