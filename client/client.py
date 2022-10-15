import socket
import random
import protocol_lib
import math

fileNames = ["long_test.txt", "test.txt", "longer_than_buffer_test.txt", "test_image.png", "medium_test_image.png"]#, "large_test_image.png"]
ingressAddressPort = ("", protocol_lib.ingressPort)
timeout = 15

def totalFileSegmentNumberGet(message):
    return message[protocol_lib.partOfFileIndex]

# This function translates the parts of a file which have already been received into a series of bytes
# If file part 0 has been received, this will return 0b10000000, with the bit for segment 0 set, but no others
# If file part 0, 2, 5 have been received, this will return 0b10100100
def write_segments_received(receivedSegmentNumbers, numBytesPartsReceived):
    segmentsReceived = 0
    for i in range(numBytesPartsReceived*8):
        if i in receivedSegmentNumbers:
            segmentsReceived = segmentsReceived & 0b1
            segmentsReceived = segmentsReceived << 1
    return segmentsReceived.to_bytes(numBytesPartsReceived, 'big')

# send_request sends a file request to ingress
def send_request(receivedSegmentNumbers, UDPClientSocket):
    chosenFile = random.choice(fileNames)
    print("Requesting file {}".format(chosenFile))
    if len(receivedSegmentNumbers) == 0:
        numBytesPartsReceived = protocol_lib.noPartsReceived.to_bytes(1, 'big')
        partsReceived = None
    else:
        # Number of bytes it will take to encode the parts which have been received
        numBytesPartsReceived = math.ceil(max(receivedSegmentNumbers) / 8)
        partsReceived = write_segments_received(receivedSegmentNumbers, numBytesPartsReceived)
        numBytesPartsReceived = numBytesPartsReceived.to_bytes(1, 'big')

    bytesToSend = (
        protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesRequest + len(chosenFile),
            (protocol_lib.fromClientMask | protocol_lib.requestMask),
            protocol_lib.noClientSelected)
        + protocol_lib.noFileSegment.to_bytes(1, 'big')
        + numBytesPartsReceived
    )
    bytesToSend += str.encode(chosenFile)
    if partsReceived != None:
        bytesToSend += partsReceived

    bytesToSend += str.encode("Client requesting file")
    # Send to server using UDP socket
    print("Sending ", bytesToSend)
    UDPClientSocket.sendto(bytesToSend, ingressAddressPort)
    UDPClientSocket.settimeout(timeout) # Timeout of 30 seconds, would be changed if put into a production environment (as opposed to testing)

def send_ack(UDPClientSocket):
    bytesToSend = protocol_lib.baseHeaderBuild(protocol_lib.numberOfHeaderBytesBase,
        (protocol_lib.fromClientMask | protocol_lib.fileAckMask),
        protocol_lib.noClientSelected)
    UDPClientSocket.sendto(bytesToSend, ingressAddressPort)
    UDPClientSocket.settimeout(None)

def setup_port():
    # Empty IP number, assigned by Docker
    ingressAddressPort = ("", protocol_lib.ingressPort)
    # Create a UDP socket
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    return UDPClientSocket

def write_file(fileSegments):
    file = bytes()
    for segment in fileSegments:
        file += segment[segment[protocol_lib.headerLengthIndex]:]

    msg = "File received.".format(file)
    print(msg)

    fileName = fileSegments[0]
    print("First 10 bytes ", fileName[:10])
    print("Think filename is ", fileName[protocol_lib.numberOfHeaderBytesBase:fileName[protocol_lib.headerLengthIndex]])
    fileName = fileName[protocol_lib.numberOfHeaderBytesBase:fileName[protocol_lib.headerLengthIndex]].decode()

    outputFile = open(r"../output/{}".format(fileName), "w")
    outputFile = open(r"../output/{}".format(fileName), "wb")
    outputFile.write(file)
    outputFile.close()

def receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers):
    try:
        while True:
            message = UDPClientSocket.recvfrom(protocol_lib.bufferSize)[0]
            print("Received something")
            fileSegments.append(message)
            fileSegmentNumber = message[protocol_lib.partOfFileIndex]
            receivedSegmentNumbers.append(fileSegmentNumber)

            # If it is the final segment
            if message[protocol_lib.actionSelectorIndex] & protocol_lib.notFinalSegmentMask != protocol_lib.notFinalSegmentMask:
                totalFileSegmentNumber = fileSegmentNumber

            if len(fileSegments) == totalFileSegmentNumber:
                fileSegments.sort(key=totalFileSegmentNumberGet)
                return

    except TimeoutError:
        print("Receiving timed out, trying to receive again")
        receivedSegmentNumbers.sort()
        send_request(receivedSegmentNumbers, UDPClientSocket)
        UDPClientSocket.settimeout(timeout)
        receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers)

UDPClientSocket = setup_port()
send_request([], UDPClientSocket)

# File segment list, to store file segments while waiting for others to arrive
fileSegments = []
totalFileSegmentNumber = -1
receivedSegmentNumbers = []

receive_file_segments(UDPClientSocket, fileSegments, totalFileSegmentNumber, receivedSegmentNumbers)
write_file(fileSegments)
