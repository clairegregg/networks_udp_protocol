import socket
import random

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

fileNames = ["long_test.txt", "test.txt", "longer_than_buffer_test.txt"] #
chosenFile = random.choice(fileNames)
bytesToSend = (baseHeaderBuild(numberOfHeaderBytesBase + len(chosenFile), fromClientMask, noClientSelected)
    + noFileSegment.to_bytes(1, 'big')
    + str.encode(chosenFile + "Client requesting file"))

# Empty IP number, assigned by Docker
serverAddressPort = ("", 20001)

# Create a UDP socket
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Send to server using UDP socket
UDPClientSocket.sendto(bytesToSend, serverAddressPort)

# Client : file segment dictionary, to store file segments while waiting for others to arrive
fileSegments = []
fileSegmentNumber = -1

def fileSegmentNumberGet(message):
    return message[partOfFileIndex]

while True:
    message = UDPClientSocket.recvfrom(bufferSize)[0]
    fileSegments.append(message)

    # If it is the final segment
    if message[actionSelectorIndex] & notFinalSegmentMask != notFinalSegmentMask:
        fileSegmentNumber = message[partOfFileIndex] + 1

    if len(fileSegments) == fileSegmentNumber:
        fileSegments.sort(key=fileSegmentNumberGet)
        break

file = bytes()
for segment in fileSegments:
    file += segment[segment[headerLengthIndex]:]

msg = "File received: {}".format(file)
print(msg)
