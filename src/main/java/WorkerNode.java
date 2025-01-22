import java.io.*;
import java.net.*;
import java.util.*;

public class WorkerNode implements Runnable {
    private static final String MASTER_IP = "192.168.126.197"; // Change to Master Node's IP
    private static final int PORT = 5000;

    public static void main(String[] args) {
        WorkerNode workerNode = new WorkerNode();
        new Thread(workerNode).start();
    }

    @Override
    public void run() {
        try (Socket socket = new Socket(MASTER_IP, PORT)) {
            System.out.println("Connected to Master Node: " + socket.getInetAddress().getHostAddress());
            processChunks(socket);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void processChunks(Socket socket) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

        while (true) {
            try {
                List<Integer> chunk = (List<Integer>) in.readObject();
                if (chunk == null) {
                    break; // Exit when master sends 'null'
                }

                System.out.println("Worker received chunk: " + chunk);  // Log received chunk
                Collections.sort(chunk);
                System.out.println("Worker sorted chunk: " + chunk);  // Log sorted chunk

                out.writeObject(chunk);
                out.flush();
            } catch (EOFException e) {
                System.out.println("End of stream reached, worker exiting.");
                break;
            }
        }
    }
}
