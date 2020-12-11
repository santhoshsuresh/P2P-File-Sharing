import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class peerProcess {
    static AtomicInteger peerId = new AtomicInteger();

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
    final static String DIR_NAME = "peer_";
    private final static String HANDSHAKE_HEADER = "P2PFILESHARINGPROJ";
    private final static String HANDSHAKE_ZEROS = "0000000000";

    //Helper variables
    static final Map<Integer, peerConnected> connectedPeerMap = new ConcurrentHashMap<>();
    static final Map<Integer, Boolean> connectedPeerSet = new ConcurrentHashMap<>();
    static final Map<Integer, Double> peer_DownloadRate = new ConcurrentHashMap<>();
    static final Map<Integer, Boolean> interestedPeersMap = new ConcurrentHashMap<>();
    static List<Integer> interestedButChokedPeers = new CopyOnWriteArrayList<>();
    static List<Double> pieceRequested;
    static AtomicInteger optimisticallyUnchokedNeighbor = new AtomicInteger();
    static AtomicInteger completedPeers = new AtomicInteger(0);
    static AtomicBoolean hasCompleted = new AtomicBoolean(false);

    //PeerDetails
    static int numOfChunks = 0;
    static Map<Integer, Integer> bitField;
    static int peerCount = 0;
    static int portNumber;
    static String hostName;
    static boolean isLastPeer = false;
    static AtomicBoolean hasFile = new AtomicBoolean();

    // Delete old directory and log files if exits and create new ones
    private static void createLogAndDir() throws IOException {
        File logFile = new File(LOG_FILE_PREFIX + peerId + LOG_FILE_SUFFIX);
        if(logFile.exists() && logFile.delete())
            System.out.println("Log file deleted");

        if (logFile.createNewFile())
            System.out.println("Log file created");

        File dirPath = new File(DIR_NAME + peerId);
        boolean isDirectoryCreated = dirPath.mkdir();

        if (isDirectoryCreated) {
            System.out.println("successfully made");
        } else {
            deleteDir(dirPath);  // Invoke recursive method
            System.out.println("Old directory deleted");
            dirPath.mkdir();
        }

        System.out.println("Log and Directory are created");
    }

    // Recursive function to delete all old files in peers directory
    private static void deleteDir(File dirPath) {
        File[] oldFiles = dirPath.listFiles();

        if (oldFiles != null) {
            for (File toBeDeletedFiles: oldFiles) {
                if (toBeDeletedFiles.isDirectory()) {
                    deleteDir(toBeDeletedFiles);
                }
                toBeDeletedFiles.delete();
            }
        }
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
        bitField = new ConcurrentHashMap<>(numOfChunks);
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
        int lastPeerId = peerId.get();
        List<String> values;

        while ((line = br.readLine()) != null) {
            values = Arrays.asList(line.split(" "));
            int curPeerId = Integer.parseInt(values.get(0));
            String curServerName = values.get(1);
            int curPort = Integer.parseInt(values.get(2));
            boolean curhasFile = "1".equals(values.get(3));

            if(curPeerId == peerId.get()) {
                int fillValue = 0;

                // If current peer has hasfile set to 1, copy the data file and create chunks
                if (curhasFile) {
                    fillValue = 1;
                    splitFiles();
                    String logmsg = peerProcess.logPrefix() + " has downloaded the complete file";
                    insertLog(logmsg);
                    hasCompleted.set(true);
                }

                for(int i=1; i<=numOfChunks; i++){
                    bitField.put(i, fillValue);
                }
                portNumber = curPort;
                hostName = curServerName;
                hasFile.set(curhasFile);
                System.out.println("has file is "+ hasFile);
            }
            else {
                peerConnected curPeer = new peerConnected(curPeerId, curServerName, curPort, numOfChunks, curhasFile);
                connectedPeerMap.put(curPeerId, curPeer);
            }
            lastPeerId = curPeerId;
            peerCount++;
        }
        if(peerId.get() == lastPeerId){
            isLastPeer = true;
        }
    }

    public synchronized static String logPrefix() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return "[" + dtf.format(now) + "]: Peer " + peerId;
    }

    private static void splitFiles(){
        long curPieceSize = 0;
        int curChunk = 1, readData;
        try {
            String fileHeader = fileName.split("\\.", 2)[0];
            File source = new File(fileName);
            File dest = new File(DIR_NAME + peerId + "/" + fileName);

            Files.copy(source.toPath(), dest.toPath());

            FileInputStream fInputStream = new FileInputStream(source);
            InputStream infile = new BufferedInputStream(fInputStream);
            readData = infile.read();

            while (readData != -1 && curChunk <= numOfChunks) {
                File destPiece = new File(DIR_NAME + peerId + "/" + fileHeader + curChunk + ".dat");
                OutputStream outfile = new BufferedOutputStream(new FileOutputStream(destPiece));
                while (readData != -1 && curPieceSize < pieceSize) {
                    outfile.write(readData);
                    curPieceSize++;
                    readData = infile.read();
                }
                curPieceSize = 0;
                outfile.close();
                curChunk++;
            }
            infile.close();
            fInputStream.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Program Execution Started");
        peerId.set(Integer.parseInt(args[0]));
        System.out.println("Current Peer Id is " + peerId);
        createLogAndDir();
        loadCommonCfg();
        loadPeers();
        pieceRequested = new CopyOnWriteArrayList<>(Collections.nCopies(numOfChunks, 0.0));

        //Send handshake message to other peers from peers connected map
        Thread initiateConnRunnable = new Thread(new ClientHandShakeRunnable());
        initiateConnRunnable.start();

        //Accept HandShake from other peers
        if(!isLastPeer){
            Thread acceptConnection = new Thread(new ServerHandshakeRunnable());
            acceptConnection.start();
        }

        // Spawn a new thread to find best neighbors
        Thread findBestNeigh = new Thread(new findBestNeighbors());
        findBestNeigh.start();

        // Spawn a new thread to find optimistically best neighbor
        Thread findOptimNeigh = new Thread(new findOptimisticallyBestNeighbors());
        findOptimNeigh.start();

        while (completedPeers.get() < peerCount-1 || !hasCompleted.get()) {
            Thread.sleep(1000);
        }
        insertLog("System Exiting");
        System.out.println("System Exiting");
        System.exit(0);
    }

    // Parse the header and peerId form handshake message
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

                    if (neighborPeerId >= peerId.get())
                        continue;
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
                        connectedPeerSet.put(neighborPeerId, true);

                        // Start the data exchange between current and selected neighboring peer
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

                        // Start the data exchange between current and requested neighboring peer
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

    public synchronized static void insertLog(String message) {
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

    static class findBestNeighbors implements Runnable {

        @Override
        public void run() {

            // Terminate selection of neighbors when current and all neighbors have the complete file
            while (completedPeers.get() < peerCount-1 || !hasCompleted.get()) {
                System.out.println("Interested peer map size is " + interestedPeersMap.size());
                System.out.println("Completed neighbors " + completedPeers.get() + " out of " + (peerCount-1));

                // Create a list to retreive all interested peers
                List<Integer> interestedPeers = new ArrayList<>();
                for(int curPeerId: interestedPeersMap.keySet()){
                    if(interestedPeersMap.containsKey(curPeerId) && interestedPeersMap.get(curPeerId)){
                        interestedPeers.add(curPeerId);
                    }
                }

                int interestedPeerCount = interestedPeers.size();

                System.out.println("Interested peer count is " + interestedPeerCount + " and pref neigh count is " + numPreferredNeighbors);
                StringBuilder logmsg = new StringBuilder(logPrefix() + " has the preferred neighbors");

                // Send unchoke to all peers if the count of interested peers < preferred neighbors count
                if (interestedPeerCount <= numPreferredNeighbors) {
                    for (int nxtPeerId : interestedPeers) {
                        peerConnected nxtPeerObj = connectedPeerMap.get(nxtPeerId);
                        logmsg.append(" ").append(nxtPeerId).append(",");
                        nxtPeerObj.sendUnchoke();
                    }
                    interestedButChokedPeers = new CopyOnWriteArrayList<>();
                }

                // Else sort the interested peers by download rate, unchoke the top K ones and choke the rest
                else if(hasCompleted.get()){
                    List<Integer> chokedPeers = new ArrayList<>();
                    Random randIdx = new Random();

                    for (int i = 0; i < numPreferredNeighbors; i++) {
                        int nxtPeerIdx = randIdx.nextInt(interestedPeers.size());
                        int nxtPeerId = interestedPeers.get(nxtPeerIdx);
                        peerConnected nxtPeerObj = connectedPeerMap.get(nxtPeerId);
                        logmsg.append(" ").append(nxtPeerId).append(",");
                        nxtPeerObj.sendUnchoke();
                        interestedPeers.remove(nxtPeerIdx);
                    }
                    for(int nxtPeerId: interestedPeers){
                        peerConnected nxtPeerObj = connectedPeerMap.get(nxtPeerId);
                        nxtPeerObj.sendChoke();
                        chokedPeers.add(nxtPeerId);
                    }
                    interestedButChokedPeers = new CopyOnWriteArrayList<>(chokedPeers);
                }

                else {
                    List<Map.Entry<Integer, Double>> preferredPeers = new LinkedList<>(peer_DownloadRate.entrySet());
                    List<Integer> chokedPeers = new ArrayList<>();
                    // Sort the list
                    preferredPeers.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
//                    System.out.println("sorted order is " + preferredPeers);

                    for (int i = 0; i < preferredPeers.size(); i++) {
                        int nxtPeerId = preferredPeers.get(i).getKey();
                        peerConnected nxtPeerObj = connectedPeerMap.get(nxtPeerId);
                        if (i < numPreferredNeighbors) {
                            nxtPeerObj.sendUnchoke();
                            logmsg.append(" " + nxtPeerId + ",");
                        }
                        else {
                            nxtPeerObj.sendChoke();
                            chokedPeers.add(nxtPeerId);
                        }
                    }
                    System.out.println("Size of interested but choked neighbors are " + chokedPeers.size());
                    interestedButChokedPeers = new CopyOnWriteArrayList<>(chokedPeers);
                }
                logmsg.setLength(logmsg.length() - 1);
                insertLog(logmsg.toString());
                System.out.println(logmsg.toString());
                try {
                    Thread.sleep(unChokingInterval*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Thread findBestNeighbors has ended");
        }
    }

    static class findOptimisticallyBestNeighbors implements Runnable {

        @Override
        public void run() {
            try {

                // Terminate selection of neighbors when current and all neighbors have the complete file
                while (completedPeers.get() < peerCount-1 || !hasCompleted.get()) {
                    System.out.println("Size of interestedButChokedPeers is " + interestedButChokedPeers.size());
                    if(interestedButChokedPeers.size()>0){
                        Random rand = new Random();
                        int nxtPeerIdx = rand.nextInt(interestedButChokedPeers.size());
                        int nxtPeerId = interestedButChokedPeers.get(nxtPeerIdx);
                        peerConnected nxtPeerObj = connectedPeerMap.get(nxtPeerId);
                        optimisticallyUnchokedNeighbor.set(nxtPeerId);
                        nxtPeerObj.sendUnchoke();

                        String logmsg = logPrefix() + " has the optimistically unchoked neighbor " + nxtPeerId;
                        System.out.println(logmsg);
                        insertLog(logmsg);
                    }
                    Thread.sleep(optimisticUnChokingInterval * 1000);
                }
                System.out.println("Thread findOptimisticallyBestNeighbors has ended");
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
