import socket

# common variables which i can't figure out how to import
fromClientMask = 0b1000
fromWorkerMask = 0b100
fromWorkerDeclarationMask = 0b101
fromIngressMask = 0b10
bufferSize = 1024

ingressAddressPort = ("", 20001)

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Declare worker
bytesToSend = 0b10.to_bytes(1, 'big') + fromWorkerDeclarationMask.to_bytes(1, 'big') + str.encode("Worker declaring itself to ingress")
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

    # Sending a reply to ingress
    bytesToSend = 0b11.to_bytes(1, 'big') + fromWorkerMask.to_bytes(1, 'big') + message[2].to_bytes(1, 'big') + str.encode("Worker sending response back to ingress")
    UDPWorkerSocket.sendto(bytesToSend, ingressAddressPort)
