import java.io.*;
import java.net.*;
import java.util.*;

public class WorkerNode implements Runnable {
    private static final String MASTER_IP = "192.168.126.197";  // IP of the master node
    private static final int PORT = 5000; // Port used for worker connection
    private Socket masterSocket;

    public static void main(String[] args) {
        WorkerNode workerNode = new WorkerNode();
        new Thread(workerNode).start();
    }

    @Override
    public void run() {
        try {
            // Connect to the master node
            masterSocket = new Socket(MASTER_IP, PORT);
            System.out.println("Connected to Master Node: " + masterSocket.getInetAddress().getHostAddress());

            // Wait for the signal to start sorting (the sort trigger)
            waitForSortSignal();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void waitForSortSignal() {
        try {
            // Wait for the signal (sort trigger) from the MasterNode
            while (true) {
                // Read data (chunk) from the master node
                Integer[] chunk = receiveDataFromMaster();

                if (chunk != null) {
                    // Once data is received, sort it
                    Arrays.sort(chunk);

                    // Send the sorted chunk back to the master node
                    sendSortedDataToMaster(chunk);
                    System.out.println("Sorted chunk sent back to Master Node");
                } else {
                    System.out.println("No data received, exiting worker.");
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Integer[] receiveDataFromMaster() throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(masterSocket.getInputStream())) {
            return (Integer[]) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Error receiving data from master", e);
        }
    }

    private void sendSortedDataToMaster(Integer[] sortedData) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(masterSocket.getOutputStream())) {
            out.writeObject(sortedData);
            out.flush();
        }
    }
}