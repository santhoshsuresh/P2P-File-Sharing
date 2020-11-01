import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private final static String PEER_CONFIG_FILE = "PeerInfo.cfg";
    private final static String LOG_FILE_PREFIX = "log_peer_";
    private final static String LOG_FILE_SUFFIX = ".log";
    private final static String DIR_NAME = "peer_";
    private final static String HANDSHAKE_HEADER = "P2PFILESHARINGPROJ";
    private final static String HANDSHAKE_ZEROS = "0000000000";

    //Helper variables
    private static ConcurrentHashMap<Integer, peerConnected> peer_Connected_Map = new ConcurrentHashMap<>();
    static int numOfChunks = 0;
    static int[] bitField;

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
            boolean curHasFile = Boolean.parseBoolean(values.get(3));
            if(curPeerId == peerId) {
                bitField = new int[numOfChunks];
                if (curHasFile)
                    Arrays.fill(bitField, 1);
                else
                    Arrays.fill(bitField, 0);
            }
            else {
                peerConnected curPeer = new peerConnected(curPeerId, curServerName, curPort, curHasFile, numOfChunks);
                peer_Connected_Map.put(curPeerId, curPeer);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        peerId = Integer.parseInt(args[0]);
        createLogAndDir();
        loadCommonCfg();
        loadPeers();

        //Send handshake message to other peers from peers connected map
        Thread initiateConnRunnable = new Thread(new InitiateHandShake());
        initiateConnRunnable.start();
    }

    //Initialize handshake from Client
    private static class InitiateHandShake implements Runnable {

        private Socket client_Socket = null;
        private DataInputStream input   = null;
        private DataOutputStream output = null;

        //Create Hand Shake byte array with header data and peer id
        private byte[] createHandShakePacket(int peerId) {
            byte[] packet = new byte[32];

            String message = HANDSHAKE_HEADER + HANDSHAKE_ZEROS + peerId;
            packet = message.getBytes();
            return packet;
        }

        @Override
        public void run() {
            try {
                for(Map.Entry<Integer, peerConnected> entry: peer_Connected_Map.entrySet()) {
                    int curPeerId = entry.getKey();

                    //Extract peer details to connect
                    peerConnected curPeerObj = entry.getValue();
                    String hostName = curPeerObj.getServerName();
                    int portNumber = curPeerObj.getPortNumber();

                    //Create socket and send handshake message
                    client_Socket = new Socket(hostName, portNumber);
                    output = new DataOutputStream(client_Socket.getOutputStream());
                    output.flush();
                    output.write(createHandShakePacket(curPeerId));
                    System.out.println(peerId + " sent handshake message to " + curPeerId);
                    client_Socket.close();

                    String message = logPrefix() + " makes a connection to Peer " + curPeerId + ".";
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

    private static String logPrefix() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now) + ":Peer " + peerId;
    }

    private static void insertLog(String message) {
        String Filename = LOG_FILE_PREFIX + peerId + LOG_FILE_SUFFIX;
        try {
            BufferedWriter fWrite = new BufferedWriter(new FileWriter(fileName));
            fWrite.write(message);
            fWrite.close();
        }
        catch (IOException e) {
            System.out.println("Cannot open/write to file " + Filename);
        }
    }
}
