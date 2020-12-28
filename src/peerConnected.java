import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.File;

class peerConnected {
    int connectedPeerId;
    String serverName;
    int portNumber;
    int numOfChunks;
    int[] connectedPeerBitField;

    //Connection variables
    Socket webSocket;
    peerConnected peerObj;
    public DataInputStream inputStream;
    public DataOutputStream outputStream;
    boolean hasConnectedPeerCompleted;
    boolean isChoked = true;
    AtomicBoolean isActive = new AtomicBoolean(false);

    //Message Types
    public static int CHOKE_TYPE = 0;
    public static int UNCHOKE_TYPE = 1;
    public static int INTERESTED_TYPE = 2;
    public static int NOTINTERESTED_TYPE = 3;
    public static int HAVE_TYPE = 4;
    public static int BITFIELD_TYPE = 5;
    public static int REQUEST_TYPE = 6;
    public static int PIECE_TYPE = 7;

    public peerConnected(int peerId, String serverName, int portNumber, int numOfChunks, boolean hasFile){
        this.connectedPeerId = peerId;
        this.serverName = serverName;
        this.portNumber = portNumber;
        this.numOfChunks = numOfChunks;
        hasConnectedPeerCompleted = hasFile;
        connectedPeerBitField = new int[numOfChunks];
        printConfig();
    }

    public void printConfig() {
        System.out.println("-----------------------------------");
        System.out.println("Peer ID is " + connectedPeerId);
        System.out.println("Server name is " + serverName);
        System.out.println("Port Number is " + portNumber);
        System.out.println("-----------------------------------\n");
    }

    public String getServerName() {
        return serverName;
    }

    public int getPortNumber() {
        return portNumber;
    }

    // Spawns a thread to communicate with the socket passed in the args
    public void startDataExchange(Socket webSocket) throws IOException {
        System.out.println("Data exchange initialized from " + peerProcess.peerId + " to " + connectedPeerId);
        this.webSocket = webSocket;
        inputStream = new DataInputStream(webSocket.getInputStream());
        outputStream = new DataOutputStream(webSocket.getOutputStream());

        String logmsg = peerProcess.logPrefix() + " started its data exchange with " + connectedPeerId;
        peerProcess.insertLog(logmsg);

        //isActive turns true only when the data exchange is initiated
        isActive.set(true);
        Thread peerDataExchange = new Thread(new DataExchangeUtil(this), "ThreadToPeer_"+connectedPeerId);
        peerDataExchange.start();
    }

    class DataExchangeUtil implements Runnable {

        public DataExchangeUtil(peerConnected curPeer) {
            peerObj = curPeer;
        }

        @Override
        public void run() {
            try {
                sendBitField();

                // Communicate with the 'connectedPeerId' until both the current and 'connectedPeerId' has the complete file
                while (!hasConnectedPeerCompleted || !peerProcess.hasCompleted.get()) {

                    if(inputStream.available() > 0) {

                        //Read the message length
                        int msgLen = inputStream.readInt();
                        byte[] message = new byte[msgLen];

                        // Read the incoming packet
                        double startTime = System.nanoTime();
                        inputStream.readFully(message);
                        double endTime = System.nanoTime();

                        int msgType = extractMessageType(message[0]);
                        int payloadSize = msgLen - 1;
                        calculateDownloadRate(startTime, endTime, msgLen);

                        // Send 'Interested' message if the bit received is complete
                        if (msgType == BITFIELD_TYPE) {
                            byte[] payload = extractPayload(message, payloadSize);
                            connectedPeerBitField = convertBytesToInts(payload, payloadSize / 4);
                            String logmsg = peerProcess.logPrefix() + " received the 'bitfield' message from " + connectedPeerId;
                            peerProcess.insertLog(logmsg);
                            System.out.println("Received bitfield from " + connectedPeerId);

                            if (hasCompleteFile()) {
                                hasConnectedPeerCompleted = true;
                                if(!peerProcess.hasFile.get()) {
                                    System.out.println("Sending Interested to " + connectedPeerId);
                                    sendInterested();
                                }
                            } else {
                                sendNotInterested();
                                System.out.println("Sending Not interested to " + connectedPeerId);
                            }
                        }

                        else if (msgType == INTERESTED_TYPE) {
                            System.out.println("Received interested from " + connectedPeerId);
                            peerProcess.interestedPeersMap.put(connectedPeerId, true);
                            String logmsg = peerProcess.logPrefix() + " received the 'interested' message from " + connectedPeerId;
                            peerProcess.insertLog(logmsg);
                        }

                        else if (msgType == NOTINTERESTED_TYPE) {
                            System.out.println("Received Not Interested from " + connectedPeerId);
                            String logmsg = peerProcess.logPrefix() + " received the 'not interested' message from " + connectedPeerId;
                            peerProcess.insertLog(logmsg);
                            peerProcess.interestedPeersMap.put(connectedPeerId, false);
                            if (peerProcess.optimisticallyUnchokedNeighbor.get() == connectedPeerId)
                                peerProcess.optimisticallyUnchokedNeighbor.set(0);
                        }

                        else if (msgType == UNCHOKE_TYPE) {
                            System.out.println("Received Unchoke from " + connectedPeerId);
                            String logmsg = peerProcess.logPrefix() + " is unchoked by " + connectedPeerId;
                            peerProcess.insertLog(logmsg);
                            sendRequest();
                        }

                        else if (msgType == CHOKE_TYPE) {
                            isChoked = true;
                            String logmsg = peerProcess.logPrefix() + " is choked by " + connectedPeerId;
                            peerProcess.insertLog(logmsg);
                        }

                        else if (msgType == REQUEST_TYPE) {
                            if (!isChoked) {
                                byte[] payload = extractPayload(message, payloadSize);
                                int reqPiece = ByteBuffer.wrap(payload).getInt();
                                String logmsg = peerProcess.logPrefix() + " received the 'request' message from " + connectedPeerId + " for the piece " + reqPiece;
                                peerProcess.insertLog(logmsg);
                                sendPiece(reqPiece);
                            }
                        }

                        else if (msgType == PIECE_TYPE) {
                            byte[] payload = extractPayload(message, payloadSize);
                            byte[] pieceIdxByte = new byte[4];
                            System.arraycopy(payload, 0, pieceIdxByte, 0, 4);
                            int pieceIdx = ByteBuffer.wrap(pieceIdxByte).getInt();
                            System.out.println("Received piece " + pieceIdx + " from " + connectedPeerId);

                            // Receive the piece only if it is not available
                            if (peerProcess.bitField.get(pieceIdx) == 0) {
                                System.out.println("Downloading piece " + pieceIdx + " from " + connectedPeerId);
                                peerProcess.bitField.put(pieceIdx, 1);

                                byte[] pieceByte = new byte[payloadSize - 4];
                                System.arraycopy(payload, 4, pieceByte, 0, payloadSize - 4);

                                String path = getFilePath(pieceIdx);
                                FileOutputStream ofStream = new FileOutputStream(path);
                                ofStream.write(pieceByte);
                                ofStream.close();

                                boolean hasParentPeerCompleted = true;
                                int completedPieces = 0;
                                for (int i : peerProcess.bitField.values()) {
                                    if (i == 0) {
                                        hasParentPeerCompleted = false;
                                    } else {
                                        completedPieces++;
                                    }
                                }

                                String logMsgBuilder = peerProcess.logPrefix() + " has downloaded the piece " + pieceIdx + " from " + connectedPeerId +
                                        ". Now the number of pieces it has is " + completedPieces;
                                peerProcess.insertLog(logMsgBuilder);

                                // If the parent peer has completed, merge all the pieces and send 'have'' to all peers
                                if (hasParentPeerCompleted) {
                                    String logmsg = peerProcess.logPrefix() + " has downloaded the complete file";
                                    peerProcess.insertLog(logmsg);
                                    System.out.println(logmsg);

                                    combinePieces();
                                    System.out.println("Size of connectedPeerMap is " + peerProcess.connectedPeerMap.size());

                                    peerProcess.hasCompleted.set(true);
                                } else
                                    sendRequest();

                                for (peerConnected curPeerObj : peerProcess.connectedPeerMap.values()) {
                                    if (curPeerObj.isActive.get())
                                        curPeerObj.sendHave(pieceIdx);
                                }
                            }
                        }

                        // Update bitfield of the connected peer and check if it has received all files
                        else if (msgType == HAVE_TYPE) {
                            byte[] payload = extractPayload(message, payloadSize);
                            byte[] pieceIdxByte = new byte[4];
                            System.arraycopy(payload, 0, pieceIdxByte, 0, 4);
                            int pieceIdx = ByteBuffer.wrap(pieceIdxByte).getInt();
                            connectedPeerBitField[pieceIdx - 1] = 1;
                            System.out.println("Received have message from " + connectedPeerId + " for piece " + pieceIdx);

                            String logmsg = peerProcess.logPrefix() + " received the ‘have’ message from " + connectedPeerId + " for the piece " + pieceIdx;
                            peerProcess.insertLog(logmsg);

                            if (hasCompleteFile()) {
                                hasConnectedPeerCompleted = true;
                                peerProcess.interestedPeersMap.put(connectedPeerId, false);
                                if (peerProcess.optimisticallyUnchokedNeighbor.get() == connectedPeerId)
                                    peerProcess.optimisticallyUnchokedNeighbor.set(0);
                                System.out.println(connectedPeerId + " has completed.");
                            }
                            if (hasInterestingPieces())
                                sendInterested();
                        }

                        else {
                            System.out.println("Wrong message type received");
                        }
                    }
                }
                peerProcess.completedPeers.incrementAndGet();
                String logmsg = peerProcess.logPrefix() + " is ending its connection with " + connectedPeerId;
                peerProcess.insertLog(logmsg);
                System.out.println("Connection completed for " + connectedPeerId);

            } catch (IOException e) {
                System.out.println("Exception 1 caught for peer " + connectedPeerId);
                peerProcess.insertLog(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // Merges all the chunk files received
    public synchronized void combinePieces() throws IOException {
        String fileName = peerProcess.fileName;
        File combinedFile = new File(System.getProperty("user.dir") + File.separator + "peer_" + peerProcess.peerId.get()
                + File.separator+fileName);
        FileOutputStream fos = new FileOutputStream(combinedFile);
        File[] splitFiles = new File[numOfChunks];

        for(int curChunk=1;curChunk<=numOfChunks;curChunk++) {
            splitFiles[curChunk-1] = new File(System.getProperty("user.dir") + File.separator + "peer_" +peerProcess.peerId.get() + File.separator + fileName + curChunk);
        }

        for(int i=0;i<numOfChunks;i++) {
            FileInputStream fis = new FileInputStream(splitFiles[i]);
            int lengthOfChunkFile = (int)splitFiles[i].length();
            byte[] readChunkFile = new byte[lengthOfChunkFile];
            fis.read(readChunkFile);
            fos.write(readChunkFile);
            fis.close();
        }
        fos.close();
    }

    // Check if the parent peer ha completed
    public synchronized boolean hasCompleteFile(){
        for(int i: connectedPeerBitField){
            if(i == 0){
                return false;
            }
        }
        return true;
    }

    // Checks if the connected peer has pieces that the parent peer is interested in
    public synchronized boolean hasInterestingPieces(){
        for(int i=1; i<=numOfChunks; i++){
            if(connectedPeerBitField[i-1] == 1 && peerProcess.bitField.get(i) == 0)
                return true;
        }
        return false;
    }

    // Calculates the download rate based on start and end time
    public synchronized void calculateDownloadRate(double startTime, double endTime, int msgLength){
        double diff = (endTime - startTime) / 1000F;
        double downloadRate = msgLength / diff;
        peerProcess.peer_DownloadRate.put(connectedPeerId, downloadRate);
    }

    public synchronized byte[] convertIntsToBytes(int[] dataLoad) {
        int index = 0;
        byte[] result = new byte[dataLoad.length * 4];
        for(int i: dataLoad){
            byte[] temp = ByteBuffer.allocate(4).putInt(i).array();
            for(int j=0; j<4; j++){
                result[index] = temp[j];
                index++;
            }
        }
        return result;
    }

    public synchronized int[] convertBytesToInts(byte[] dataLoad, int length) {
        int index = 0;
        int[] output = new int[length];
        while(index < length){
            byte[] temp = new byte[4];
            System.arraycopy(dataLoad, index*4, temp, 0, 4);
            output[index] = ByteBuffer.wrap(temp).getInt();
            index++;
        }
        return output;
    }

    public synchronized void sendBitField() throws IOException {
        int[] bitFieldArray = new int[numOfChunks];
        for(int i=1; i<=numOfChunks; i++){
            bitFieldArray[i-1] = peerProcess.bitField.get(i);
        }
        byte[] bFArray = convertIntsToBytes(bitFieldArray);
        byte[] packet = createPacket(BITFIELD_TYPE, bFArray);
        outputStream.flush();
        outputStream.write(packet);
        String logmsg = peerProcess.logPrefix() + " is sending the 'bitfield' message to " + connectedPeerId;
        peerProcess.insertLog(logmsg);
        System.out.println("Sent bitfield to " + connectedPeerId + " from " + peerProcess.peerId);
    }

    public synchronized byte[] createPacket(int messageType, byte[] payload) {
        byte msgTypeByte = (byte) (char) messageType;
        int payloadLength = 0;
        if(payload != null)
            payloadLength = payload.length;
        int messageLen = 1 + payloadLength;
        byte[] msgLenBytes = ByteBuffer.allocate(4).putInt(messageLen).array();
        ByteBuffer packet = ByteBuffer.allocate(msgLenBytes.length + messageLen);
        packet.put(msgLenBytes);
        packet.put(msgTypeByte);
        if(payloadLength > 0)
            packet.put(payload);
        return packet.array();
    }

    public synchronized byte[] extractPayload(byte[] data, int length){
        byte[] payLoad = new byte[length];
        System.arraycopy(data, 1, payLoad, 0, length);
        return payLoad;
    }

    public synchronized int extractMessageType(byte data){
        return Byte.toUnsignedInt(data);
    }

    public synchronized void sendInterested() {
        byte[] packet = createPacket(INTERESTED_TYPE, null);
        sendPacket(packet);
    }

    public synchronized void sendNotInterested() {
        byte[] packet = createPacket(NOTINTERESTED_TYPE, null);
        sendPacket(packet);
        String logmsg = peerProcess.logPrefix() + " is sending 'Not Interested' message to " + connectedPeerId;
        peerProcess.insertLog(logmsg);
    }

    public synchronized void sendUnchoke() {
        if(isChoked || peerProcess.optimisticallyUnchokedNeighbor.get() == connectedPeerId){
            isChoked = false;
            byte[] packet = createPacket(UNCHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent unchoke to " + connectedPeerId);
            String logmsg = peerProcess.logPrefix() + " is sending 'Unhoke' message to " + connectedPeerId;
            peerProcess.insertLog(logmsg);
        }
    }

    public synchronized void sendChoke() {
        if(!isChoked){
            isChoked = true;
            byte[] packet = createPacket(CHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent choke to " + connectedPeerId);
            String logmsg = peerProcess.logPrefix() + " is sending 'Choke' message to " + connectedPeerId;
            peerProcess.insertLog(logmsg);
        }
    }

    // Sends a request message for a piece that the connected peer has and parent peer does not
    public synchronized void sendRequest() {
        List<Integer> availablePieces = new ArrayList<>();
        List<Double> pieceRequestStatus = new ArrayList<>(peerProcess.pieceRequested);

        for(int i=1; i<=numOfChunks; i++){
            double curTime = System.nanoTime();
            if(peerProcess.bitField.get(i) == 0 && connectedPeerBitField[i-1] == 1) {
                availablePieces.add(i);
            }
        }
        System.out.println("Available piece count is " + availablePieces.size());

        if(availablePieces.size() > 0) {
            Random rand = new Random();
            int idx = rand.nextInt(availablePieces.size());
            int piece = availablePieces.get(idx);
            peerProcess.pieceRequested.set(piece-1, (double) (System.currentTimeMillis()));

            byte[] pieceBytes = ByteBuffer.allocate(4).putInt(piece).array();
            byte[] packet = createPacket(REQUEST_TYPE, pieceBytes);
            sendPacket(packet);
            System.out.println("Sent Request to " + connectedPeerId + " for the piece " + piece);
            String logmsg = peerProcess.logPrefix() + " is sending 'Request' message to " + connectedPeerId + " for the piece " + piece;
            peerProcess.insertLog(logmsg);
        }
        else{
            isChoked = true;
            sendNotInterested();
        }
    }

    // Sends the piece file if the connected peer is unchoked or it is optimisticall unchoked neighbor
    public synchronized void sendPiece(int reqPiece) throws IOException {
        if((!isChoked || peerProcess.optimisticallyUnchokedNeighbor.get() == connectedPeerId) && peerProcess.bitField.get(reqPiece) == 1) {
            byte[] pieceIdx = ByteBuffer.allocate(4).putInt(reqPiece).array();
            String path = getFilePath(reqPiece);

            byte[] piece = Files.readAllBytes(Paths.get(path));

            byte[] message = new byte[pieceIdx.length + piece.length];
            System.arraycopy(pieceIdx, 0, message, 0, pieceIdx.length);
            System.arraycopy(piece, 0, message, pieceIdx.length, piece.length);

            byte[] packet = createPacket(PIECE_TYPE, message);
            sendPacket(packet);
            System.out.println("Sent Piece " + reqPiece + " to " + connectedPeerId);
            String logmsg = peerProcess.logPrefix() + " is sending 'Piece' message to " + connectedPeerId + " for the piece " + reqPiece;
            peerProcess.insertLog(logmsg);
        }
    }

    public synchronized void sendHave(int havePiece) {
        byte[] pieceIdx = ByteBuffer.allocate(4).putInt(havePiece).array();
        byte[] packet = createPacket(HAVE_TYPE, pieceIdx);
        System.out.println("Sent have msg to " + connectedPeerId + " for the piece " + havePiece);
        sendPacket(packet);
        String logmsg = peerProcess.logPrefix() + " is sending 'have' message to " + connectedPeerId + " for the piece " + havePiece;
        peerProcess.insertLog(logmsg);
    }

    public synchronized String getFilePath(int piece){
        String dirName = System.getProperty("user.dir") + File.separator + peerProcess.DIR_NAME + peerProcess.peerId.get();
        String filename = peerProcess.fileName + piece;
        return dirName + File.separator + filename;
    }

    public synchronized void sendPacket(byte[] packet) {
        try {
            outputStream.flush();
            outputStream.write(packet);
        } catch (IOException e) {
            peerProcess.insertLog(e.getMessage());
            e.printStackTrace();
        }
    }
}