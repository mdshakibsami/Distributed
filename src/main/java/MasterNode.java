import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MasterNode implements Runnable {
    private static final int PORT = 5000;
    private final List<Socket> workerSockets = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        MasterNode masterNode = new MasterNode();
        new Thread(masterNode).start();
    }

    @Override
    public void run() {
        startServer();
    }

    private void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Master Node is running on port " + PORT);

            while (true) {
                Socket workerSocket = serverSocket.accept();
                workerSockets.add(workerSocket);
                System.out.println("Worker connected: " + workerSocket.getInetAddress().getHostAddress());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getWorkerIPs() {
        return workerSockets.stream()
                .map(socket -> socket.getInetAddress().getHostAddress())
                .collect(Collectors.toList());
    }

    public List<Integer> sortAndMergeData(List<Integer> data) {
        if (workerSockets.isEmpty()) {
            throw new IllegalStateException("No workers are connected.");
        }

        // Split data into chunks
        int chunkSize = (int) Math.ceil((double) data.size() / workerSockets.size());
        List<List<Integer>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += chunkSize) {
            chunks.add(new ArrayList<>(data.subList(i, Math.min(i + chunkSize, data.size()))));  // Convert subList to ArrayList
        }

        // Send chunks to workers and collect results
        List<Future<List<Integer>>> futures = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            final Socket workerSocket = workerSockets.get(i);
            final List<Integer> chunk = chunks.get(i);
            System.out.println("Sending chunk to worker " + (i + 1) + ": " + chunk);  // Log chunk being sent
            futures.add(executor.submit(() -> processChunk(workerSocket, chunk)));
        }

        // Wait for results and merge sorted chunks
        List<Integer> sortedData = new ArrayList<>();
        for (Future<List<Integer>> future : futures) {
            try {
                sortedData.addAll(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Sorting operation was interrupted.");
                return Collections.emptyList();
            } catch (ExecutionException e) {
                System.out.println("An error occurred during sorting: " + e.getMessage());
                e.printStackTrace();
                return Collections.emptyList();
            }
        }

        Collections.sort(sortedData);
        System.out.println("Final sorted data: " + sortedData);  // Log the final sorted data
        return sortedData;
    }

    private List<Integer> processChunk(Socket workerSocket, List<Integer> chunk) throws IOException, ClassNotFoundException {
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {

            out.writeObject(chunk);
            out.flush();

            return (List<Integer>) in.readObject();
        }
    }

    public void sendEndOfStream() {
        for (Socket workerSocket : workerSockets) {
            try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream())) {
                out.writeObject(null);  // Send 'null' to signal end of processing
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
