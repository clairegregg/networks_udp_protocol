# Masks for Action bit of header
fromClientMask = 0b1000
fromWorkerMask = 0b100
declarationMask = 0b1
fromIngressMask = 0b10
notFinalSegmentMask = 0b10000
requestMask = 0b100000
fileAckMask = 0b1000000

# Indices of sections of the header
headerLengthIndex = 0
actionSelectorIndex = 1
clientIndex = 2
partOfFileIndex = 3
bytesOfReceivedPartsIndex = 4 # This only applies if the request mask is set

# Values for when a byte is not set in header
noClientSelected = 0b0
noFileSegment = 0b0
noPartsReceived = 0

numberOfHeaderBytesBase = 0b100
numberOfHeaderBytesRequest = 5
bufferSize = 65507
ingressPort = 20001

def baseHeaderBuild(length, actionSelector, client):
    return length.to_bytes(1, 'big') + actionSelector.to_bytes(1, 'big') + client.to_bytes(1, 'big')
