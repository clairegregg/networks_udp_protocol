import socket

localIP = ""
localPort = 10001

msgFromClient = "Hello UDP Server"
bytesToSend = str.encode(msgFromClient)
# Empty IP number, assigned by Docker
serverAddressPort = ("", 20001)
bufferSize = 1024

# Create a UDP socket
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind socket to IP and port
UDPClientSocket.bind((localIP, localPort))

# Send to server using UDP socket
UDPClientSocket.sendto(bytesToSend, serverAddressPort)

msgFromServer = UDPClientSocket.recvfrom(bufferSize)

msg = "Message from Server {}".format(msgFromServer[0])
print(msg)
