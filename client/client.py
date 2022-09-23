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

fileNames = ["test.txt", "long_test.txt"]#, "longer_than_buffer_test.txt"]
chosenFile = random.choice(fileNames)
bytesToSend = (baseHeaderBuild(numberOfHeaderBytesBase + len(chosenFile), fromClientMask, noClientSelected)
    + str.encode(chosenFile + "Client requesting file"))

# Empty IP number, assigned by Docker
serverAddressPort = ("", 20001)
bufferSize = 1024

# Create a UDP socket
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Send to server using UDP socket
UDPClientSocket.sendto(bytesToSend, serverAddressPort)

msgFromServer = UDPClientSocket.recvfrom(bufferSize)

msg = "Message from Server: {}".format(msgFromServer[0])
print(msg)
