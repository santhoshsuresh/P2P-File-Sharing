import java.io.*;
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
    private final static String COMMON_CONFIG_FILE = "src/Common.cfg";
    private final static String PEER_CONFIG_FILE = "src/PeerInfo.cfg";
    private final static String LOG_FILE_PREFIX = "log_peer_";
    private final static String DIR_NAME = "peer_";

    private static ConcurrentHashMap<Integer, peerConnected> peer_Connected_Map = new ConcurrentHashMap<>();

    private static void createLogAndDir() throws IOException {
        File logFile = new File(LOG_FILE_PREFIX + peerId);
        logFile.createNewFile();

        File dirName = new File(DIR_NAME + peerId);
        dirName.mkdir();
    }

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
        printCommonCfg();
    }

    private static void printCommonCfg(){
        System.out.println("-----------------------------------");
        System.out.println("Preferred neighbors are " + numPreferredNeighbors);
        System.out.println("Unchoking Interval is " + unchokingInterval);
        System.out.println("Optimistic Unchoking Interval is " + optimisticUnchokingInterval);
        System.out.println("FileName is " + fileName);
        System.out.println("PieceSize is " + pieceSize);
        System.out.println("FileSize is " + fileSize);
        System.out.println("-----------------------------------\n");
    }

    private static void loadPeers() throws IOException {
        File file = new File(PEER_CONFIG_FILE);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        List<String> values = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            values = Arrays.asList(line.split(" "));
            int curPeerId = Integer.parseInt(values.get(0));
            String curServerName = values.get(1);
            int curPort = Integer.parseInt(values.get(2));
            boolean curHasFile = Boolean.parseBoolean(values.get(3));
            peerConnected curPeer = new peerConnected(curPeerId, curServerName, curPort, curHasFile);
            peer_Connected_Map.put(curPeerId, curPeer);
        }
    }

    public static void main(String args[]) throws IOException {
        peerId = Integer.parseInt(args[0]);
        createLogAndDir();
        loadCommonCfg();
        loadPeers();
    }
}
