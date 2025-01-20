import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;

public class MasterNode implements Runnable {
    private static final int PORT = 5000; // Port for worker connections
    private static final List<Socket> workerSockets = new ArrayList<>();
    private static final List<int[]> chunks = new ArrayList<>();
    private static final List<Integer> sortedData = new ArrayList<>();
    private volatile boolean sortTriggered = false;
    private final List<String> workerIPs = new ArrayList<>();

    public static void main(String[] args) {
        MasterNode masterNode = new MasterNode();
        new Thread(masterNode).start();
    }

    @Override
    public void run() {
        acceptWorkerConnections();
    }

    private void acceptWorkerConnections() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Master Node is running... Waiting for workers to connect.");

            while (true) {
                Socket workerSocket = serverSocket.accept();
                synchronized (workerSockets) {
                    workerSockets.add(workerSocket);
                    workerIPs.add(workerSocket.getInetAddress().getHostAddress());
                    System.out.println("Worker connected: " + workerSocket.getInetAddress().getHostAddress());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getWorkerIPs() {
        synchronized (workerIPs) {
            return new ArrayList<>(workerIPs);
        }
    }

    public void triggerSort(List<Integer> data) {
        synchronized (this) {
            if (workerSockets.isEmpty()) {
                System.out.println("No workers connected to perform sorting.");
                return;
            }

            // Split data into chunks
            int chunkSize = data.size() / workerSockets.size();
            for (int i = 0; i < workerSockets.size(); i++) {
                int start = i * chunkSize;
                int end = (i == workerSockets.size() - 1) ? data.size() : start + chunkSize;
                chunks.add(data.subList(start, end).stream().mapToInt(Integer::intValue).toArray());
            }

            sortTriggered = true;
            sortData();
        }
    }

    private void sortData() {
        try {
            long startTime = System.currentTimeMillis();

            // Distribute chunks to workers
            for (int i = 0; i < workerSockets.size(); i++) {
                sendDataToWorker(workerSockets.get(i), chunks.get(i));
            }

            // Collect sorted chunks from workers
            for (Socket workerSocket : workerSockets) {
                sortedData.addAll(Arrays.asList(receiveDataFromWorker(workerSocket)));
            }

            // Merge sorted data
            sortedData.sort(Integer::compareTo);
            long endTime = System.currentTimeMillis();

            System.out.println("Sorting completed in " + (endTime - startTime) + " ms");
            displaySortedData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendDataToWorker(Socket worker, int[] data) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(worker.getOutputStream());
        out.writeObject(data);
        out.flush();
    }

    private Integer[] receiveDataFromWorker(Socket worker) throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(worker.getInputStream())) {
            return (Integer[]) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Error receiving data from worker", e);
        }
    }

    private void displaySortedData() {
        System.out.println("Sorted Data:");
        sortedData.forEach(num -> System.out.print(num + " "));
    }
}
