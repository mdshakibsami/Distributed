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
import java.util.stream.Collectors;

public class NodeManagerGUI extends Application {
    private final MasterNode masterNode = new MasterNode();
    private final ListView<String> workerListView = new ListView<>();
    private final TextArea statusTextArea = new TextArea();
    private List<Integer> data;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Distributed Sorting System");

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

        refreshWorkerList();
    }

    private void uploadFile(Stage primaryStage) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Text Files", "*.txt"));

        File file = fileChooser.showOpenDialog(primaryStage);
        if (file != null) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                data = reader.lines()
                        .flatMap(line -> Arrays.stream(line.split("\\s+")))
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                statusTextArea.appendText("File uploaded: " + file.getName() + "\n");
            } catch (IOException e) {
                statusTextArea.appendText("Error reading file: " + e.getMessage() + "\n");
            }
        } else {
            statusTextArea.appendText("No file selected.\n");
        }
    }

    private void triggerSort() {
        if (data == null || data.isEmpty()) {
            statusTextArea.appendText("No data to sort. Please upload a file first.\n");
            return;
        }

        try {
            List<Integer> sortedData = masterNode.sortAndMergeData(data);
            statusTextArea.appendText("Sorted Data: " + sortedData + "\n");

            // Write to output file
            File outputFile = new File("sorted_result.txt");
            try (PrintWriter writer = new PrintWriter(outputFile)) {
                for (int num : sortedData) {
                    writer.println(num);
                }
            }
            statusTextArea.appendText("Result written to: " + outputFile.getAbsolutePath() + "\n");
        } catch (Exception e) {
            statusTextArea.appendText("Error during sorting: " + e.getMessage() + "\n");
        }
    }

    private void refreshWorkerList() {
        List<String> workerIPs = masterNode.getWorkerIPs();
        Platform.runLater(() -> workerListView.getItems().setAll(workerIPs));
        statusTextArea.appendText("Worker list refreshed.\n");
    }
}
