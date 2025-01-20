import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import java.io.*;
import java.util.*;


public class NodeManagerGUI extends Application {
    private final MasterNode masterNode = new MasterNode(); // Connect MasterNode
    private final ListView<String> workerListView = new ListView<>();
    private final TextArea statusTextArea = new TextArea();

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Distributed Sorting System");

        // Start MasterNode in a separate thread
        new Thread(masterNode).start();

        Button uploadButton = new Button("Upload File");
        Button sortButton = new Button("Sort Data");
        Button refreshButton = new Button("Refresh Workers");

        uploadButton.setOnAction(e -> uploadFile(primaryStage));
        sortButton.setOnAction(e -> triggerSort());
        refreshButton.setOnAction(e -> refreshWorkerList());

        VBox centerBox = new VBox(10, new Label("Connected Workers:"), workerListView, new Label("Status:"), statusTextArea);
        HBox buttonBox = new HBox(10, uploadButton, sortButton, refreshButton);

        BorderPane root = new BorderPane();
        root.setTop(buttonBox);
        root.setCenter(centerBox);

        Scene scene = new Scene(root, 800, 600);
        primaryStage.setScene(scene);
        primaryStage.show();

        // Auto-refresh worker list periodically
        refreshWorkerList();
    }

    private void uploadFile(Stage primaryStage) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Text Files", "*.txt"));

        File file = fileChooser.showOpenDialog(primaryStage);
        if (file != null) {
            statusTextArea.appendText("File uploaded: " + file.getName() + "\n");
        } else {
            statusTextArea.appendText("No file selected.\n");
        }
    }

    private void triggerSort() {
        List<Integer> data = Arrays.asList(5, 3, 8, 6, 2); // Replace with actual file data
        masterNode.triggerSort(data);
        statusTextArea.appendText("Sort triggered.\n");
    }

    private void refreshWorkerList() {
        List<String> workerIPs = masterNode.getWorkerIPs();
        workerListView.getItems().setAll(workerIPs);
        statusTextArea.appendText("Worker list refreshed.\n");
    }
}
