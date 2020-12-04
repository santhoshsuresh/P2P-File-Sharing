import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class peerProcess {
    static int peerId;

    //CommonConfig data
    static int numPreferredNeighbors;
    static int unChokingInterval;
    static int optimisticUnChokingInterval;
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
    static final Map<Integer, peerConnected> connectedPeerMap = new ConcurrentHashMap<>();
    static final Map<Integer, Boolean> connectedPeerSet = new ConcurrentHashMap<>();
    static final Map<Integer, Double> peer_DownloadRate = new ConcurrentHashMap<>();
    static final CopyOnWriteArrayList<Integer> interestedPeers = new CopyOnWriteArrayList<>();
    static AtomicInteger optimisticallyUnchokedNeighbor = new AtomicInteger();
    static AtomicInteger completedPeers = new AtomicInteger(0);

    //PeerDetails
    static int numOfChunks = 0;
    static int[] bitField;
    static int peerCount = 0;
    static int portNumber;
    static String hostName;
    static boolean isLastPeer = false;
    static AtomicBoolean hasFile = new AtomicBoolean();

    private static void createLogAndDir() throws IOException {
        File logFile = new File(LOG_FILE_PREFIX + peerId + LOG_FILE_SUFFIX);
        if(logFile.exists() && logFile.delete())
            System.out.println("Log file deleted");

        if (logFile.createNewFile())
            System.out.println("Log file created");

        File dirName = new File(DIR_NAME + peerId);
        if (dirName.exists() && dirName.delete())
            System.out.println("Directory deleted");

        if (dirName.mkdir())
            System.out.println("Directory created");
        System.out.println("Log and Directory are created");
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
        unChokingInterval = Integer.parseInt(values.get(1));
        optimisticUnChokingInterval = Integer.parseInt(values.get(2));
        fileName = values.get(3);
        fileSize = Long.parseLong(values.get(4));
        pieceSize = Long.parseLong(values.get(5));

        numOfChunks = calculateNumOfChunk(fileSize, pieceSize);
        bitField = new int[numOfChunks];
        System.out.println("Common config file loaded");
        printCommonCfg();
    }

    private static int calculateNumOfChunk(long fSize, long pSize){
        double chunkSize = Math.ceil(1.0 * fSize / pSize);
        return (int) chunkSize;
    }

    private static void printCommonCfg(){
        System.out.println("-----------------------------------");
        System.out.println("Preferred neighbors are " + numPreferredNeighbors);
        System.out.println("Unchoking Interval is " + unChokingInterval);
        System.out.println("Optimistic Unchoking Interval is " + optimisticUnChokingInterval);
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
        int lastPeerId = peerId;
        List<String> values;
        while ((line = br.readLine()) != null) {
            values = Arrays.asList(line.split(" "));
            int curPeerId = Integer.parseInt(values.get(0));
            String curServerName = values.get(1);
            int curPort = Integer.parseInt(values.get(2));
            hasFile.set("1".equals(values.get(3)));

            if(curPeerId == peerId) {
                bitField = new int[numOfChunks];
                if (hasFile.get()) {
                    Arrays.fill(bitField, 1);
//                    Files.copy(source.toPath(), dest.toPath());
                    splitFiles();
                    completedPeers.incrementAndGet();
                }
                else
                    Arrays.fill(bitField, 0);
                portNumber = curPort;
                hostName = curServerName;
            }
            else {
                peerConnected curPeer = new peerConnected(curPeerId, curServerName, curPort, numOfChunks);
                connectedPeerMap.put(curPeerId, curPeer);
            }
            lastPeerId = curPeerId;
            peerCount++;
        }
        if(peerId == lastPeerId){
            isLastPeer = true;
        }
    }

    private static String logPrefix() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return "[" + dtf.format(now) + "]: Peer " + peerId;
    }

    private static void splitFiles(){
        long curPieceSize = 0;
        int curChunk = 1, readData;
        try {
            String fileHeader = fileName.split("\\.", 2)[0];
//            System.out.println(fileHeader + "_" + fileName);
            File source = new File(fileName);
            FileInputStream fInputStream = new FileInputStream(source);
            InputStream infile = new BufferedInputStream(fInputStream);
            readData = infile.read();

            while (readData != -1 && curChunk < numOfChunks) {
                File dest = new File((DIR_NAME + peerId + "/" + fileHeader + curChunk + ".dat"));
                OutputStream outfile = new BufferedOutputStream(new FileOutputStream(dest));
                while (readData != -1 && curPieceSize < pieceSize) {
                    outfile.write(readData);
                    curPieceSize++;
                    readData = infile.read();
                }
                curPieceSize = 0;
                outfile.close();
                curChunk++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Program Execution Started");
        peerId = Integer.parseInt(args[0]);
        System.out.println("Current Peer Id is " + peerId);
        createLogAndDir();
        loadCommonCfg();
        loadPeers();

        //Send handshake message to other peers from peers connected map
        Thread initiateConnRunnable = new Thread(new ClientHandShakeRunnable());
        initiateConnRunnable.start();

        //Accept HandShake from other peers
        if(!isLastPeer){
            Thread acceptConnection = new Thread(new ServerHandshakeRunnable());
            acceptConnection.start();
        }
    }

    private static String[] parseMessage(byte[] buffer) {
        String[] output = new String[2];
        byte[] header = new byte[18];
        byte[] peerId = new byte[4];
        System.arraycopy(buffer, 0, header, 0, 18);
        System.arraycopy(buffer, 28, peerId, 0, 4);

        output[0] = new String(header);
        output[1] = new String(peerId);
        return output;
    }

    //Create Hand Shake byte array with header data and peer id
    private static byte[] createHandShakeSegment() {
        String message = HANDSHAKE_HEADER + HANDSHAKE_ZEROS + peerId;
        return message.getBytes();
    }

    //Initialize handshake from Client
    private static class ClientHandShakeRunnable implements Runnable {

        @Override
        public void run() {
            try {
                System.out.println("Entering client socket for " + peerId);
                for(Map.Entry<Integer, peerConnected> entry: connectedPeerMap.entrySet()) {
                    int neighborPeerId = entry.getKey();
                    if (neighborPeerId >= peerId)
                        break;
                    System.out.println(peerId + " trying to connect " + neighborPeerId);
                    //Extract peer details to connect
                    peerConnected curPeerObj = entry.getValue();
                    String hostName = curPeerObj.getServerName();
                    int portNumber = curPeerObj.getPortNumber();

                    //Create socket and send handshake message
                    Socket client_Socket = new Socket(hostName, portNumber);
                    DataOutputStream clientOutputStream = new DataOutputStream(client_Socket.getOutputStream());
                    clientOutputStream.flush();
                    clientOutputStream.write(createHandShakeSegment());

                    connectedPeerSet.put(neighborPeerId, true);
                    String message = logPrefix() + " makes a connection to Peer " + neighborPeerId + ".";
                    System.out.println(message);
                    insertLog(message);

                    //Accept the response from the server
                    DataInputStream clientInputStream = new DataInputStream(client_Socket.getInputStream());
                    byte[] inputStream = new byte[32];
                    clientInputStream.readFully(inputStream);
                    String[] parseInput = parseMessage(inputStream);
                    int receivedPeerId = Integer.parseInt(parseInput[1]);

                    if (parseInput[0].equals(HANDSHAKE_HEADER) && receivedPeerId == neighborPeerId) {
                        message = logPrefix() + " completed hand shake with Peer " + neighborPeerId + ".";
                        insertLog(message);
                        connectedPeerSet.put(neighborPeerId, true);
                        curPeerObj.startDataExchange(client_Socket);
                        System.out.println(message);
                    }
                }
                System.out.println("Exiting client socket for " + peerId);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("IO Exception found in run of hs 1");
                e.printStackTrace();
            }
        }
    }

    private static class ServerHandshakeRunnable implements Runnable {

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(portNumber);
                byte[] receiveBuffer = new byte[32];

                System.out.println("Entering server accept zone for " + peerId);

                while (connectedPeerSet.size() < peerCount - 1) {
                    System.out.println(connectedPeerSet.size() + "- Entering - " + (peerCount - 1));
                    Socket server_Socket = serverSocket.accept();
                    DataInputStream serverInputStream = new DataInputStream(server_Socket.getInputStream());
                    serverInputStream.readFully(receiveBuffer);

                    String[] parseInput = parseMessage(receiveBuffer);
                    int neighborPeerId = Integer.parseInt(parseInput[1]);
                    System.out.println("Received segment from " + neighborPeerId);

                    if (parseInput[0].equals(HANDSHAKE_HEADER) && !connectedPeerSet.containsKey(neighborPeerId)) {
                        connectedPeerSet.put(neighborPeerId, true);
                        String message = logPrefix() + " is connected from Peer " + neighborPeerId + ".";
                        insertLog(message);
                        System.out.println(message);

                        //Send hand shake message back to the client
                        DataOutputStream serverOutputStream = new DataOutputStream(server_Socket.getOutputStream());
                        serverOutputStream.flush();
                        serverOutputStream.write(createHandShakeSegment());
                        peerConnected neighborPeerObj = connectedPeerMap.get(neighborPeerId);
                        neighborPeerObj.startDataExchange(server_Socket);
                    }
                }
                System.out.println(connectedPeerSet.size() + " - Exiting - " + (peerCount - 1));
                System.out.println("Exiting server accept zone for " + peerId);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private static void insertLog(String message) {
        String Filename = LOG_FILE_PREFIX + peerId + LOG_FILE_SUFFIX;
        try {
            BufferedWriter fWrite = new BufferedWriter(new FileWriter(Filename, true));
            fWrite.write(message + "\n");
            fWrite.close();
        }
        catch (IOException e) {
            System.out.println("Cannot open/write to file " + Filename);
        }
    }
}
