import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
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
    boolean peerComplete = false;
    AtomicBoolean isChoked = new AtomicBoolean(false);

    //Message Types
    public static int CHOKE_TYPE = 0;
    public static int UNCHOKE_TYPE = 1;
    public static int INTERESTED_TYPE = 2;
    public static int NOTINTERESTED_TYPE = 3;
    public static int HAVE_TYPE = 4;
    public static int BITFIELD_TYPE = 5;
    public static int REQUEST_TYPE = 6;
    public static int PIECE_TYPE = 7;

    public peerConnected(int peerId, String serverName, int portNumber, int numOfChunks){
        this.connectedPeerId = peerId;
        this.serverName = serverName;
        this.portNumber = portNumber;
//        this.hasFile = hasFile;
        this.numOfChunks = numOfChunks;

        connectedPeerBitField = new int[numOfChunks];
//        loadBitfield(hasFile);
        printConfig();
    }

//    private void loadBitfield(boolean hasFile){
//        if (hasFile)
//            Arrays.fill(bitField, 1);
//        else
//            Arrays.fill(bitField, 0);
//    }

    public void printConfig() {
        System.out.println("-----------------------------------");
        System.out.println("Peer ID is " + connectedPeerId);
        System.out.println("Server name is " + serverName);
        System.out.println("Port Number is " + portNumber);
//        System.out.println("Has file is " + hasFile);
        System.out.println("-----------------------------------\n");
    }

    public int getPeerId() {
        return connectedPeerId;
    }

    public String getServerName() {
        return serverName;
    }

    public int getPortNumber() {
        return portNumber;
    }

//    public boolean isHasFile() {
//        return hasFile;
//    }

    public int[] getBitField() {
        return connectedPeerBitField;
    }

    public void startDataExchange(Socket webSocket) throws IOException {
        System.out.println("Data exchange initialized from " + peerProcess.peerId + " to " + connectedPeerId);
        this.webSocket = webSocket;
        inputStream = new DataInputStream(webSocket.getInputStream());
        outputStream = new DataOutputStream(webSocket.getOutputStream());
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

                while (peerProcess.completedPeers.get() < peerProcess.peerCount) {

                    int msgLen = inputStream.readInt();
                    byte[] message = new byte[msgLen];

                    long startTime = System.currentTimeMillis();
                    inputStream.readFully(message);
                    long endTime = System.currentTimeMillis();

                    int msgType = extractMessageType(message[0]);
                    int payLoadSize = msgLen-1;
                    calculateDownloadRate(startTime, endTime, msgLen);
//                    System.out.println("length is " + msgLen + " type is " + msgType);
                    System.out.println("Message type received is " + msgType);
                    if (msgType == BITFIELD_TYPE) {
                        byte[] payload = extractPayload(message, payLoadSize);
                        connectedPeerBitField = convertBytesToInts(payload, payLoadSize / 4);
                        boolean hasFile = true;
                        for(int i: connectedPeerBitField){
                            if(i == 0){
                                hasFile = false;
                                break;
                            }
                        }
                        System.out.println("Received bitfield from " + connectedPeerId);
                        if(hasFile && !peerProcess.hasFile.get()){
                            peerComplete = true;
                            System.out.println("Sending Interested to " + connectedPeerId);
                            sendInterested();
                        }
                        else {
                            sendNotInterested();
                            System.out.println("Sending Not interested to " + connectedPeerId);
                        }
                    }

                    else if(msgType == INTERESTED_TYPE){
                        System.out.println("Received interested from " + connectedPeerId);
                        peerProcess.interestedPeers.add(connectedPeerId);
                    }

                    else if(msgType == NOTINTERESTED_TYPE){
                        System.out.println("Received Not Interested from " + connectedPeerId);
                        if(peerProcess.interestedPeers.contains(connectedPeerId))
                            peerProcess.interestedPeers.remove(connectedPeerId);
                        if(peerProcess.optimisticUnChokingInterval == connectedPeerId)
                            peerProcess.optimisticUnChokingInterval = 0;
                    }

                    else if(msgType == UNCHOKE_TYPE){
                        System.out.println("Received Unchoke from " + connectedPeerId);
                        List<Integer> availablePieces = new ArrayList<>();
                        List<Integer> parentBitField = new ArrayList<>(peerProcess.bitField.keySet());
                        List<Double> pieceRequestStatus = new ArrayList<>(peerProcess.pieceRequested);

                        for(int i=0; i<numOfChunks; i++){
                            double curTime = System.currentTimeMillis() / 1000F;
                            if(parentBitField.get(i) == 0 && connectedPeerBitField[i] == 1
                                    && (curTime - pieceRequestStatus.get(i)) > 2.0) {
                                availablePieces.add(i);
                            }
                        }
                        if(availablePieces.size() > 0) {
                            Random rand = new Random();
                            int piece = availablePieces.get(rand.nextInt(availablePieces.size()));
//                            sendRequest(piece);
                        }
                    }

                }
                System.out.println("Connection completed for " + connectedPeerId);
//                peerConnected.
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
        for(int i=0; i<numOfChunks; i++){
            bitFieldArray[i] = peerProcess.bitField.get(i);
        }
        byte[] bFArray = convertIntsToBytes(bitFieldArray);
        byte[] packet = createPacket(BITFIELD_TYPE, bFArray);
        System.out.println("Packet length is "+ packet.length);
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

    public synchronized void sendInterested() throws IOException {
        byte[] packet = createPacket(INTERESTED_TYPE, null);
        sendPacket(packet);
    }

    public synchronized void sendNotInterested() throws IOException {
        byte[] packet = createPacket(NOTINTERESTED_TYPE, null);
        sendPacket(packet);
    }

    public synchronized void sendUnchoke() throws IOException {
        if(!isChoked.get()){
            byte[] packet = createPacket(UNCHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent unchoke to " + connectedPeerId);
        }
    }

    public synchronized void sendChoke() throws IOException {
        if(isChoked.get()){
            byte[] packet = createPacket(CHOKE_TYPE, null);
            sendPacket(packet);
            System.out.println("Sent choke to " + connectedPeerId);
        }
    }

    public synchronized void sendRequest(int piece) throws IOException {
        byte[] pieceBytes = ByteBuffer.allocate(4).putInt(piece).array();
        byte[] packet = createPacket(REQUEST_TYPE, pieceBytes);
        sendPacket(packet);
    }

    public synchronized void sendPacket(byte[] packet) throws IOException {
        outputStream.flush();
        outputStream.write(packet);
    }
}