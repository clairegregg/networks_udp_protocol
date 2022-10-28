# Author: Claire Gregg

# Masks for Control bit of header
declarationMask     = 0b1
fromWorkerMask      = 0b10
fromClientMask      = 0b100
notFinalSegmentMask = 0b1000
requestMask         = 0b10000
fileAckMask         = 0b100000

# Number of bytes the file part can take up
numberOfFilePartBytes = 2

# Indices of sections of the header
headerLengthIndex       = 0
controlIndex            = 1
clientIndex             = 2
partOfFileStartIndex      = 3
bytesOfReceivedPartsStartIndex = partOfFileStartIndex + numberOfFilePartBytes # This only applies if the request mask is set

# Values for when a byte is not set in header
noClientSelected    = 0
noFileSegment       = 0
noPartsReceived     = 0

numberOfHeaderBytesBase     = 3 + numberOfFilePartBytes
numberOfHeaderBytesRequest  = 3 + (numberOfFilePartBytes*2)
bufferSize = 65507
ingressPort = 20001

def getFilePart(msg):
    return int.from_bytes(msg[partOfFileStartIndex:partOfFileStartIndex+numberOfFilePartBytes], 'big')

def getReceivedPartsBytes(msg):
    return int.from_bytes(msg[bytesOfReceivedPartsStartIndex:bytesOfReceivedPartsStartIndex+numberOfFilePartBytes], 'big')

def baseHeaderBuild(length, control, client, partIndex):
    return length.to_bytes(1, 'big') + control.to_bytes(1, 'big') + client.to_bytes(1, 'big') + partIndex.to_bytes(numberOfFilePartBytes, 'big')
