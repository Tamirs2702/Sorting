package com.example;

import com.example.MultiThreadedCSVSorter;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import javassist.bytecode.analysis.Executor;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import java.io.Console;
import java.io.FileReader;
import java.util.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.logging.Level;

import static com.fasterxml.jackson.databind.type.LogicalType.Map;
import static org.junit.Assert.assertTrue;

public class MultiThreadedCSVSorterTest {
    private static final Logger LOGGER = Logger.getLogger(MultiThreadedCSVSorterTest.class.getName());

    @Test
    public void testExecutionTime() {
        long startTime = System.currentTimeMillis();

        // Update these paths to your test CSV files
        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        String outputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\a.csv";
//        String inputPath = "path_to_input.csv";
//        String outputPath = "path_to_output.csv";

        MultiThreadedCSVSorter.main(new String[]{inputPath, outputPath});

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Execution time of MultiThreadedCSVSorter: " + duration + " milliseconds");

        // You can also assert if the time is below a certain threshold, but it's not typical for unit tests
        // long maxExpectedDuration = ...; // Define your threshold
        // Assert.assertTrue("Execution time exceeded the expected duration", duration < maxExpectedDuration);
    }
    @Test

    public void testSortingWithinChunks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        List<Path> chunks = MultiThreadedCSVSorter.splitAndSortChunks(inputPath, executor);

        for (Path chunkPath : chunks) {
            assertTrue("Chunk should be sorted", isChunkSorted(chunkPath));
        }

        // Clean up - delete chunk files
        for (Path chunk : chunks) {
            Files.deleteIfExists(chunk);
        }
    }

    private boolean isChunkSorted(Path chunkPath) throws IOException {
        try (CSVReader reader = new CSVReader(new FileReader(chunkPath.toFile()))) {
            String[] previousLine = null;
            String[] currentLine;

            while ((currentLine = reader.readNext()) != null) {
                if (previousLine != null) {
                    // Compare previous line with current line based on the sorting column (here, index 0)
                    int previousValue = Integer.parseInt(previousLine[1]);
                    int currentValue = Integer.parseInt(currentLine[1]);

                    if (previousValue > currentValue) {
                        return false; // The chunk is not sorted
                    }
                }
                previousLine = currentLine;
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true; // The chunk is sorted
    }

    @Test
    public void testDifferentMaxRowsValues() {
        int[] maxRowsValues = {2, 5, 10, 20}; // Different values to test
        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        String outputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\a.csv";
        Map<Integer, Long> executionTimes = new HashMap<>();

        for (int maxRows : maxRowsValues) {
            MultiThreadedCSVSorter sorter = new MultiThreadedCSVSorter(maxRows);

            long startTime = System.currentTimeMillis();
            MultiThreadedCSVSorter.main(new String[]{inputPath, outputPath}); // Simulate the main method call
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            executionTimes.put(maxRows, duration);

            LOGGER.info("Execution time with MAX_ROWS = " + maxRows + ": " + (endTime - startTime) + " milliseconds");
            logSummary(executionTimes);

        }

    }
    private void logSummary(Map<Integer, Long> executionTimes) {
        LOGGER.info("----- Summary of Execution Times -----");
        executionTimes.forEach((maxRows, time) ->
                LOGGER.info("MAX_ROWS = " + maxRows + ": " + time + " milliseconds")
        );
    }
}

