import socket
import random
import protocol_lib

fileNames = ["long_test.txt", "test.txt", "longer_than_buffer_test.txt", "test_image.png"] #
chosenFile = random.choice(fileNames)
bytesToSend = (protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase + len(chosenFile), protocol_lib.fromClientMask, protocol_lib.noClientSelected)
    + protocol_lib.noFileSegment.to_bytes(1, 'big')
    + str.encode(chosenFile + "Client requesting file"))

# Empty IP number, assigned by Docker
ingressAddressPort = ("", protocol_lib.ingressPort)

# Create a UDP socket
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Send to server using UDP socket
UDPClientSocket.sendto(bytesToSend, ingressAddressPort)

# File segment list, to store file segments while waiting for others to arrive
fileSegments = []
fileSegmentNumber = -1

def fileSegmentNumberGet(message):
    return message[protocol_lib.partOfFileIndex]

while True:
    message = UDPClientSocket.recvfrom(protocol_lib.bufferSize)[0]
    fileSegments.append(message)

    # If it is the final segment
    if message[protocol_lib.actionSelectorIndex] & protocol_lib.notFinalSegmentMask != protocol_lib.notFinalSegmentMask:
        fileSegmentNumber = message[protocol_lib.partOfFileIndex] + 1

    if len(fileSegments) == fileSegmentNumber:
        fileSegments.sort(key=fileSegmentNumberGet)
        break

file = bytes()
for segment in fileSegments:
    file += segment[segment[protocol_lib.headerLengthIndex]:]

msg = "File received: {}".format(file)
print(msg)

outputFile = open(r"../output/{}".format("test_image.png"), "w")
outputFile = open(r"../output/{}".format("test_image.png"), "wb")
outputFile.write(file)
outputFile.close()
