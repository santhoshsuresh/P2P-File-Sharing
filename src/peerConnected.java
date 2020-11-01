import java.util.Arrays;

public class peerConnected {
    int peerId;
    String serverName;
    int portNumber;
    boolean hasFile;
    int numOfChunks;
    int[] bitField;

    public peerConnected(int peerId, String serverName, int portNumber, boolean hasFile, int numOfChunks){
        this.peerId = peerId;
        this.serverName = serverName;
        this.portNumber = portNumber;
        this.hasFile = hasFile;
        this.numOfChunks = numOfChunks;

        bitField = new int[numOfChunks];
        loadBitfield(hasFile);
        printConfig();
    }

    private void loadBitfield(boolean hasFile){
        if (hasFile)
            Arrays.fill(bitField, 1);
        else
            Arrays.fill(bitField, 0);
    }

    public void printConfig() {
        System.out.println("-----------------------------------");
        System.out.println("Peer ID is " + peerId);
        System.out.println("Server name is " + serverName);
        System.out.println("Port Number is " + portNumber);
        System.out.println("Has file is " + hasFile);
        System.out.println("-----------------------------------\n");
    }
}