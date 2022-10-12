import socket
import protocol_lib
import multiprocessing

def send_file(message, address):
    headerLength = message[protocol_lib.headerLengthIndex]
    fileName = message[protocol_lib.numberOfHeaderBytesBase:headerLength] # Gives file name which is after base header and before any other explanatory message

    # Sending a reply to ingress
    bytesToSend = None
    with open(fileName.decode(), "rb") as f:
        bytes_read = f.read()
        filePart = 0
        startRead = 0
        while True:
            print("Sending part {}".format(filePart))
            endRead = startRead + protocol_lib.bufferSize-headerLength

            # If this is the final segment
            if endRead > len(bytes_read):
                endRead = len(bytes_read)-1
                # Ensure notFinalSegment bit not set to represent that this is the final segment
                bytesToSend = (protocol_lib.baseHeaderBuild(headerLength, protocol_lib.fromWorkerMask,
                message[protocol_lib.clientIndex]))
                send = bytesToSend + filePart.to_bytes(1, 'big') + fileName + bytes_read[startRead:endRead]
                UDPWorkerSocket.sendto(send, ingressAddressPort)
                break

            # Set notFinalSegment bit to represent that there are more segments of this file to come
            bytesToSend = (protocol_lib.baseHeaderBuild(headerLength, protocol_lib.fromWorkerMask|protocol_lib.notFinalSegmentMask,
            message[protocol_lib.clientIndex]))
            send = bytesToSend + filePart.to_bytes(1, 'big') + fileName + bytes_read[startRead:endRead]
            UDPWorkerSocket.sendto(send, ingressAddressPort)
            startRead = endRead
            filePart += 1

ingressAddressPort = ("", protocol_lib.ingressPort)

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
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]
    msgFromIngress = "Message from ingress: {}".format(message)
    ingressIP = "Ingress IP address: {}".format(address)
    print(msgFromIngress)
    print(ingressIP)

    process = multiprocessing.Process(target=send_file, args=(message,address))
    process.start()
