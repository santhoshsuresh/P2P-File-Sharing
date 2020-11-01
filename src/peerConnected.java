public class peerConnected {
    int peerId;
    String serverName;
    int portNumber;
    boolean hasFile;

    public peerConnected(int peerId, String serverName, int portNumber, boolean hasFile){
        this.peerId = peerId;
        this.serverName = serverName;
        this.portNumber = portNumber;
        this.hasFile = hasFile;
        printConfig();
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