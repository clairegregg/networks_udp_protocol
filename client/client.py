import socket
import random
import protocol_lib
import math

# Global variables

# Options of files to get
fileNames = ["test.txt", "test_image.jpg", "long_test.txt",  "longer_than_buffer_test.txt",  "medium_test_image.png", "large_test_image.jpg", "test_video.mp4", "test_gif.gif"]

# Address port to request files from
ingressAddressPort = ("", protocol_lib.ingressPort)

# Port timeout (in seconds). Can only be this low as it is in testing.
timeout = 2

# Choose the file from the list for testing.
chosenFile = random.choice(fileNames)

# Function to give a key for sorting file segments.
def totalFileSegmentNumberGet(message):
    return message[protocol_lib.partOfFileIndex]

# This function translates the parts of a file which have already been received into a series of bytes
# If file part 0 has been received, this will return 0b10000000, with the bit for segment 0 set, but no others
# If file part 0, 2, 5 have been received, this will return 0b10100100
def write_segments_received(receivedSegmentNumbers, numBytesPartsReceived):
    segmentsReceived = 0
    for i in range((numBytesPartsReceived*8)-1):
        if i in receivedSegmentNumbers:
            segmentsReceived = segmentsReceived | 0b1
        segmentsReceived = segmentsReceived << 1
    return segmentsReceived.to_bytes(numBytesPartsReceived, 'big')

# send_request sends a file request to ingress based on which segments it has already received
def send_request(receivedSegmentNumbers, UDPClientSocket):
    # If you have not received any sections, or this is the first request
    partsReceived = 0
    if len(receivedSegmentNumbers) <= 0:
        numBytesPartsReceived = protocol_lib.noPartsReceived
    else:
        # Number of bytes it will take to encode the parts which have been received
        numBytesPartsReceived = math.ceil(max(receivedSegmentNumbers) / 8)
        partsReceived = write_segments_received(receivedSegmentNumbers, numBytesPartsReceived)

    bytesToSend = (
        protocol_lib.baseHeaderBuild(
            protocol_lib.numberOfHeaderBytesRequest + len(chosenFile),
            (protocol_lib.fromClientMask | protocol_lib.requestMask),
            protocol_lib.noClientSelected, protocol_lib.noFileSegment
        )
        + numBytesPartsReceived.to_bytes(1, 'big')
        + str.encode(chosenFile)
    )
    if len(receivedSegmentNumbers) > 0:
        bytesToSend += partsReceived

    # Send to server using UDP socket
    UDPClientSocket.sendto(bytesToSend, ingressAddressPort)
    UDPClientSocket.settimeout(timeout) # Timeout of 30 seconds, would be changed if put into a production environment (as opposed to testing)

# send_ack sends an ack to ingress so that it can release the worker
def send_ack(UDPClientSocket):
    bytesToSend = protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase,
        (protocol_lib.fromClientMask | protocol_lib.fileAckMask),
        protocol_lib.noClientSelected, protocol_lib.noFileSegment)
    UDPClientSocket.sendto(bytesToSend, ingressAddressPort)
    UDPClientSocket.settimeout(None)

# setup_port sets up the client's socket
def setup_port():
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    return UDPClientSocket

# write_file takes in the file which has been received and creates/writes it locally.
def write_file(fileSegments):
    file = bytes()
    for segment in fileSegments:
        file += segment[segment[protocol_lib.headerLengthIndex]:]

    msg = "File received.".format(file)

    fileName = fileSegments[0]
    fileName = fileName[protocol_lib.numberOfHeaderBytesBase:fileName[protocol_lib.headerLengthIndex]].decode()

    outputFile = open(r"../output/{}".format(fileName), "w")
    outputFile = open(r"../output/{}".format(fileName), "wb")
    outputFile.write(file)
    outputFile.close()

# In receive_file_segments, the client awaits file segments to be sent to it. 
# It also includes a timeout to re-request a file if it has taken too long to send, as some of the packets have probably gone missing.
def receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers):
    try:
        while True:
            # Block on receiving the message
            message = UDPClientSocket.recvfrom(protocol_lib.bufferSize)[0]

            # Add the file segment to the store of segments
            fileSegmentNumber = message[protocol_lib.partOfFileIndex]
            if fileSegmentNumber not in receivedSegmentNumbers:
                fileSegments.append(message)
                receivedSegmentNumbers.append(fileSegmentNumber)

            # If it is the final segment, store the total number of segments
            if message[protocol_lib.actionSelectorIndex] & protocol_lib.notFinalSegmentMask != protocol_lib.notFinalSegmentMask:
                totalFileSegmentNumber = fileSegmentNumber+1

            # If all file segments have arrived, return
            if len(fileSegments) == totalFileSegmentNumber:
                fileSegments.sort(key=totalFileSegmentNumberGet)
                send_ack(UDPClientSocket)
                return

    # If it has been too long since the client last received a file, request it again, then continue to wait to receive it.
    except TimeoutError:
        receivedSegmentNumbers.sort()
        send_request(receivedSegmentNumbers, UDPClientSocket)
        UDPClientSocket.settimeout(timeout)
        receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers)

##################################
###### Main part of program ######
##################################
UDPClientSocket = setup_port()
send_request([], UDPClientSocket)
print("Requesting file", chosenFile)

# File segment list, to store file segments while waiting for others to arrive
fileSegments = []
totalFileSegmentNumber = -1
receivedSegmentNumbers = []

receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers)
write_file(fileSegments)
print("Received file.")
