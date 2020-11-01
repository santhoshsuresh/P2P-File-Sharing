import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class peerProcess {
    static int peerId;

    //CommonConfig data
    static int numPreferredNeighbors;
    static int unchokingInterval;
    static int optimisticUnchokingInterval;
    static String fileName;
    static long fileSize;
    static long pieceSize;

    //Constants
    private final static String COMMON_CONFIG_FILE = "Common.cfg";
    private final static String PEER_CONFIG_FILE = "PeerInfoLocal.cfg";
    private final static String LOG_FILE_PREFIX = "log_peer_";
    private final static String LOG_FILE_SUFFIX = ".log";
    private final static String DIR_NAME = "peer_";
    private final static String HANDSHAKE_HEADER = "P2PFILESHARINGPROJ";
    private final static String HANDSHAKE_ZEROS = "0000000000";

    //Helper variables
    private static final ConcurrentHashMap<Integer, peerConnected> availablePeers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> connectedPeers = new ConcurrentHashMap<>();
    static int numOfChunks = 0;
    static int[] bitField;
    static int peerCount = 0;
    static int portNumber;
    static String hostName;

    private static void createLogAndDir() throws IOException {
        File logFile = new File(LOG_FILE_PREFIX + peerId);
        logFile.createNewFile();

        File dirName = new File(DIR_NAME + peerId);
        dirName.mkdir();
    }

    //Loads common config file data and calculates number of chunks
    private static void loadCommonCfg() throws IOException {
        File file = new File(COMMON_CONFIG_FILE);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        List<String> values = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            values.add(line.split(" ")[1]);
        }
        numPreferredNeighbors = Integer.parseInt(values.get(0));
        unchokingInterval = Integer.parseInt(values.get(1));
        optimisticUnchokingInterval = Integer.parseInt(values.get(2));
        fileName = values.get(3);
        fileSize = Long.parseLong(values.get(4));
        pieceSize = Long.parseLong(values.get(5));

        numOfChunks = calculateNumOfChunk(fileSize, pieceSize);
        bitField = new int[numOfChunks];
        printCommonCfg();
    }

    private static int calculateNumOfChunk(long fSize, long pSize){
        double chunkSize = Math.ceil(1.0 * fSize / pSize);
        return (int) chunkSize;
    }

    private static void printCommonCfg(){
        System.out.println("-----------------------------------");
        System.out.println("Preferred neighbors are " + numPreferredNeighbors);
        System.out.println("Unchoking Interval is " + unchokingInterval);
        System.out.println("Optimistic Unchoking Interval is " + optimisticUnchokingInterval);
        System.out.println("FileName is " + fileName);
        System.out.println("PieceSize is " + pieceSize);
        System.out.println("FileSize is " + fileSize);
        System.out.println("Number of Chunks are " + numOfChunks);
        System.out.println("-----------------------------------\n");
    }

    //Create mapping between peer Id and objects of peers, except for the current one
    private static void loadPeers() throws IOException {
        File file = new File(PEER_CONFIG_FILE);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        List<String> values;
        while ((line = br.readLine()) != null) {
            values = Arrays.asList(line.split(" "));
            int curPeerId = Integer.parseInt(values.get(0));
            String curServerName = values.get(1);
            int curPort = Integer.parseInt(values.get(2));
            boolean curHasFile = "1".equals(values.get(3));

            if(curPeerId == peerId) {
                bitField = new int[numOfChunks];
                if (curHasFile)
                    Arrays.fill(bitField, 1);
                else
                    Arrays.fill(bitField, 0);
                portNumber = curPort;
                hostName = curServerName;
            }
            else {
                peerConnected curPeer = new peerConnected(curPeerId, curServerName, curPort, curHasFile, numOfChunks);
                availablePeers.put(curPeerId, curPeer);
            }
            peerCount++;
        }
    }

    private static String logPrefix() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now) + ":Peer " + peerId;
    }

    public static void main(String[] args) throws IOException {
        peerId = Integer.parseInt(args[0]);
        createLogAndDir();
        loadCommonCfg();
        loadPeers();

        //Send handshake message to other peers from peers connected map
        Thread initiateConnRunnable = new Thread(new InitiateHandShake());
        initiateConnRunnable.start();

        //Accept HandShake from other peers
        Thread acceptConnection = new Thread(new ReceiveHandShake());
        acceptConnection.start();
    }

    //Initialize handshake from Client
    private static class InitiateHandShake implements Runnable {

        //Create Hand Shake byte array with header data and peer id
        private byte[] createHandShakeSegment(int peerId) {
            String message = HANDSHAKE_HEADER + HANDSHAKE_ZEROS + peerId;
            return message.getBytes();
        }

        @Override
        public void run() {
            try {
                for(Map.Entry<Integer, peerConnected> entry: availablePeers.entrySet()) {
                    int curPeerId = entry.getKey();
                    if (curPeerId > peerId)
                        break;

                    //Extract peer details to connect
                    peerConnected curPeerObj = entry.getValue();
                    String hostName = curPeerObj.getServerName();
                    int portNumber = curPeerObj.getPortNumber();

                    //Create socket and send handshake message
                    Socket client_Socket = new Socket(hostName, portNumber);
                    DataOutputStream output = new DataOutputStream(client_Socket.getOutputStream());
                    output.flush();
                    output.write(createHandShakeSegment(curPeerId));
                    client_Socket.close();

                    connectedPeers.put(curPeerId, 0);
                    String message = logPrefix() + " makes a connection to Peer " + curPeerId + ".";
                    System.out.println(message);
                    insertLog(message);
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("IO Exception found in run of hs 1");
                e.printStackTrace();
            }
        }
    }

    private static class ReceiveHandShake implements Runnable {
        private String[] parseMessage(byte[] buffer) {
            int len = buffer.length;
            String[] output = new String[3];
            ByteBuffer readBuffer = ByteBuffer.wrap(buffer);
            output[0] = String.valueOf(readBuffer.get(buffer, 0, Math.min(len, 18)));
            output[1] = String.valueOf(readBuffer.get(buffer, 18, Math.min(len - 18, 28)));
            output[2] = String.valueOf(readBuffer.get(buffer, 28, Math.min(len - 28, 32)));
            return output;
        }

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(portNumber);
                byte[] receiveBuffer = new byte[32];

                System.out.println("Entering accept zone for " + peerId);
                System.out.println(connectedPeers.size() + "-" + (peerCount - 1));

                while (connectedPeers.size() == peerCount - 1) {
                    Socket socket = serverSocket.accept();
                    DataInputStream serverInput = new DataInputStream(socket.getInputStream());
                    serverInput.readFully(receiveBuffer);
                    String[] parseInput = parseMessage(receiveBuffer);
                    int curPeerId = Integer.parseInt(parseInput[2]);
                    if (parseInput[0].equals(HANDSHAKE_HEADER) && !connectedPeers.contains(curPeerId)) {
                        connectedPeers.put(curPeerId, 0);
                        String message = logPrefix() + " is connected from Peer " + curPeerId + ".";
                        insertLog(message);
                        System.out.println(message);
                    }
                    System.out.println("Closing Server Socket for " + peerId);
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private static void insertLog(String message) {
        String Filename = LOG_FILE_PREFIX + peerId + LOG_FILE_SUFFIX;
        try {
            BufferedWriter fWrite = new BufferedWriter(new FileWriter(fileName, true));
            fWrite.write(message);
            fWrite.close();
        }
        catch (IOException e) {
            System.out.println("Cannot open/write to file " + Filename);
        }
    }
}
