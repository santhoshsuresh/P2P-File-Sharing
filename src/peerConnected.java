import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public void startDataExchange(Socket webSocket) throws IOException {
        System.out.println("Data exchange initialized from " + peerProcess.peerId + " to " + connectedPeerId);
        this.webSocket = webSocket;
        inputStream = new DataInputStream(webSocket.getInputStream());
        outputStream = new DataOutputStream(webSocket.getOutputStream());
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

                while (!hasConnectedPeerCompleted || !peerProcess.hasCompleted.get()) {
//                    System.out.println("Waiting for socket input from " + connectedPeerId);

                    if(inputStream.available() > 0) {
                        int msgLen = inputStream.readInt();
                        byte[] message = new byte[msgLen];

                        long startTime = System.currentTimeMillis();
                        inputStream.readFully(message);
                        long endTime = System.currentTimeMillis();

                        int msgType = extractMessageType(message[0]);
                        int payloadSize = msgLen - 1;
                        calculateDownloadRate(startTime, endTime, msgLen);

                        if (msgType == BITFIELD_TYPE) {
                            byte[] payload = extractPayload(message, payloadSize);
                            connectedPeerBitField = convertBytesToInts(payload, payloadSize / 4);

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
                                sendPiece(reqPiece);
                            }
                        }

                        else if (msgType == PIECE_TYPE) {
                            byte[] payload = extractPayload(message, payloadSize);
                            byte[] pieceIdxByte = new byte[4];
                            System.arraycopy(payload, 0, pieceIdxByte, 0, 4);
                            int pieceIdx = ByteBuffer.wrap(pieceIdxByte).getInt();
                            System.out.println("Received piece " + pieceIdx + " from " + connectedPeerId);

                            if (peerProcess.bitField.get(pieceIdx) == 0) {
                                System.out.println("Downloading piece " + pieceIdx + " from " + connectedPeerId);
                                byte[] pieceByte = new byte[payloadSize - 4];
                                System.arraycopy(payload, 4, pieceByte, 0, payloadSize - 4);
                                String path = getFilePath(pieceIdx);
                                FileOutputStream ofStream = new FileOutputStream(path);
                                ofStream.write(pieceByte);
                                peerProcess.bitField.put(pieceIdx, 1);

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

//                            System.out.println(peerProcess.peerId + " has completed - " + hasParentPeerCompleted);
                                if (hasParentPeerCompleted) {
                                    String logmsg = peerProcess.logPrefix() + " has downloaded the complete file";
                                    peerProcess.insertLog(logmsg);
                                    System.out.println(logmsg);
                                    for (peerConnected curPeerObj : peerProcess.connectedPeerMap.values()) {
                                        if (curPeerObj.isActive.get())
                                            curPeerObj.sendNotInterested();
                                    }
                                    peerProcess.hasCompleted.set(true);
                                } else
                                    sendRequest();

                                for (peerConnected curPeerObj : peerProcess.connectedPeerMap.values()) {
                                    if (curPeerObj.isActive.get())
                                        curPeerObj.sendHave(pieceIdx);
                                }
                            }
                        }

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
                                System.out.println(connectedPeerId + " has completed.");
                                System.out.println("hasConnectedPeerCompleted value is " + hasConnectedPeerCompleted + " for " + connectedPeerId);
                                System.out.println("peerProcess.hasCompleted.get() value is " + peerProcess.hasCompleted.get());
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
//                inputStream.close();
//                outputStream.close();
//                webSocket.close();

                System.out.println("Connection completed for " + connectedPeerId);
            } catch (IOException e) {
                System.out.println("Exception 1 caught for peer " + connectedPeerId);
                e.printStackTrace();
            }
        }
    }

    public synchronized boolean hasCompleteFile(){
        for(int i: connectedPeerBitField){
            if(i == 0){
                return false;
            }
        }
        return true;
    }

    public synchronized boolean hasInterestingPieces(){
        for(int i=1; i<=numOfChunks; i++){
            if(connectedPeerBitField[i-1] == 1 && peerProcess.bitField.get(i) == 0)
                return true;
        }
        return false;
    }

    public synchronized void calculateDownloadRate(long startTime, long endTime, int msgLength){
        double sec = (endTime - startTime) / 1000F;
        Double downloadRate = ((double) (msgLength + 1) / sec);
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
    }

    public synchronized void sendUnchoke() {
        if(isChoked){
            isChoked = false;
            byte[] packet = createPacket(UNCHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent unchoke to " + connectedPeerId);
        }
        else if(peerProcess.optimisticallyUnchokedNeighbor.get() == connectedPeerId){
            byte[] packet = createPacket(UNCHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent optimistically unchoke to " + connectedPeerId);
        }
    }

    public synchronized void sendChoke() {
        if(!isChoked){
            isChoked = true;
            byte[] packet = createPacket(CHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent choke to " + connectedPeerId);
        }
    }

    public synchronized void sendRequest() {
        List<Integer> availablePieces = new ArrayList<>();
//        List<Integer> parentBitField = new ArrayList<>(peerProcess.bitField.values());
//        List<Double> pieceRequestStatus = new ArrayList<>(peerProcess.pieceRequested);

        for(int i=1; i<=numOfChunks; i++){
            double curTime = System.currentTimeMillis() / 1000F;
            if(peerProcess.bitField.get(i) == 0 && connectedPeerBitField[i-1] == 1){
//                    && (curTime - pieceRequestStatus.get(i)) > 2.0) {
                availablePieces.add(i);
            }
        }
        System.out.println("Available piece count is " + availablePieces.size());

        if(availablePieces.size() > 0) {
            Random rand = new Random();
            int idx = rand.nextInt(availablePieces.size());
            int piece = availablePieces.get(idx);
            System.out.println("Selected piece is " + piece + " and its index is " + idx);
//            peerProcess.pieceRequested.set(piece, (double) (System.currentTimeMillis() / 1000F));

            byte[] pieceBytes = ByteBuffer.allocate(4).putInt(piece).array();
            byte[] packet = createPacket(REQUEST_TYPE, pieceBytes);
            sendPacket(packet);
            System.out.println("Sent Request to " + connectedPeerId + " for the piece " + piece);
        }
        else{
            sendNotInterested();
        }
    }

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
        }
    }

    public synchronized void sendHave(int havePiece) {
        System.out.println("Sending have piece is " + havePiece);
        byte[] pieceIdx = ByteBuffer.allocate(4).putInt(havePiece).array();
        byte[] packet = createPacket(HAVE_TYPE, pieceIdx);
        System.out.println("Sent have msg to " + connectedPeerId);
        sendPacket(packet);
    }

    public synchronized String getFilePath(int piece){
        String dirName = peerProcess.DIR_NAME + peerProcess.peerId.get();
        String filename = peerProcess.fileName.split("\\.", 2)[0] + piece + ".dat";
        return dirName + "/" + filename;
    }

    public synchronized void sendPacket(byte[] packet) {
        try {
            outputStream.flush();
            outputStream.write(packet);
        } catch (IOException e) {
            System.out.println("Exception 2" + e.getMessage() + " caught for peer " + connectedPeerId);
            e.printStackTrace();
        }
    }
}