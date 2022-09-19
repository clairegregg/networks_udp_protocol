import socket

localIP = ""
localPort = 30001
bufferSize = 1024

msgFromWorker = "Hello from UDP Worker"
bytesToSend = str.encode(msgFromWorker)

# Create a UDP socket
UDPWorkerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind socket to IP and port
UDPWorkerSocket.bind((localIP, localPort))

print("Worker UDP server up and listening")

# Listen for incoming messages
While True:
    bytesAddressPair = UDPWorkerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0]
    address = bytesAddressPair[1]
    msgFromIngress = "Message from ingress: {}".format(message)
    ingressIP = "Ingress IP address: {}".format(message)

    print(msgFromIngress)
    print(ingressIP)

    # Sending a reply to the client
    UDPWorkerSocket.sendto(bytesToSend, address)
