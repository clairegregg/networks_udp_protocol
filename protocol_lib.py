# Masks for Action bit of header
declarationMask     = 0b1
fromWorkerMask      = 0b10
fromClientMask      = 0b100
notFinalSegmentMask = 0b1000
requestMask         = 0b10000
fileAckMask         = 0b100000

# Indices of sections of the header
headerLengthIndex       = 0
actionSelectorIndex     = 1
clientIndex             = 2
partOfFileIndex         = 3
bytesOfReceivedPartsIndex = 4 # This only applies if the request mask is set

# Values for when a byte is not set in header
noClientSelected    = 0
noFileSegment       = 0
noPartsReceived     = 0

numberOfHeaderBytesBase     = 4
numberOfHeaderBytesRequest  = 5
bufferSize = 65507
ingressPort = 20001

def baseHeaderBuild(length, actionSelector, client, partIndex):
    return length.to_bytes(1, 'big') + actionSelector.to_bytes(1, 'big') + client.to_bytes(1, 'big') + partIndex.to_bytes(1, 'big')
