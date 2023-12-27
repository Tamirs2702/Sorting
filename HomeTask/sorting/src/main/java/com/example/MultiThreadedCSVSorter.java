package com.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MultiThreadedCSVSorter {

    private static int MAX_ROWS;
    public MultiThreadedCSVSorter(int maxRows) {
        MultiThreadedCSVSorter.MAX_ROWS = maxRows;
    }
    private static final Logger LOGGER = Logger.getLogger(MultiThreadedCSVSorter.class.getName());

    public static void main(String[] args) {
        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        String outputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\a.csv";
//        String inputPath = "path_to_input.csv";
//        String outputPath = "path_to_output.csv";
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try {
            long startTime = System.currentTimeMillis();  // Start time

            List<Path> sortedChunkPaths = splitAndSortChunks(inputPath, executor);
            mergeSortedChunks(sortedChunkPaths, outputPath);
            // Clean up temporary files

            long endTime = System.currentTimeMillis();  // End time
            long duration = endTime - startTime;  // Calculate duration
            for (Path path : sortedChunkPaths) {
                Files.delete(path);
            }
            LOGGER.info("CSV sorted successfully.");
            LOGGER.info("Total runtime: " + duration + " milliseconds");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } finally {
            executor.shutdown();
        }
    }

    public static List<Path> splitAndSortChunks(String inputPath, ExecutorService executor) throws IOException {
        List<Path> sortedChunkPaths = new ArrayList<>();
        List<Future<Path>> futures = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(inputPath))) {
            List<String[]> rows = new ArrayList<>();
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                rows.add(nextLine);
                if (rows.size() == MAX_ROWS) {
                    futures.add(sortAndWriteChunkAsync(rows, executor));
                    rows = new ArrayList<>();
                }
            }
            if (!rows.isEmpty()) {
                futures.add(sortAndWriteChunkAsync(rows, executor));
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        for (Future<Path> future : futures) {
            try {
                sortedChunkPaths.add(future.get());
//                LOGGER.info("Sorted chunk written to " + future.toString());

            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to sort chunk", e);
            }
        }
        return sortedChunkPaths;
    }

    private static Future<Path> sortAndWriteChunkAsync(List<String[]> chunk, ExecutorService executor) {
        return executor.submit(() -> {
            String threadName = Thread.currentThread().getName();
//            LOGGER.info("Thread " + threadName + " is sorting a chunk with " + chunk.size() + " records.");            chunk.sort(Comparator.comparing(row -> row[1])); // Sorting based on (index 1)
//            LOGGER.info("Chunk sorted and written to file.");
            Path chunkFile = Files.createTempFile("sortedChunk", ".csv");
            try (CSVWriter writer = new CSVWriter(new FileWriter(chunkFile.toFile()))) {
                for (String[] row : chunk) {
                    writer.writeNext(row);
                }
            }
//            LOGGER.info("Thread " + threadName + " has finished sorting. Chunk written to " + chunkFile);

            return chunkFile;
        });
    }

    private static void mergeSortedChunks(List<Path> sortedChunkPaths, String outputPath) throws IOException, CsvValidationException {
        PriorityQueue<CSVRecord> queue = new PriorityQueue<>();
        Map<CSVReader, String[]> openReaders = new HashMap<>();

        for (Path path : sortedChunkPaths) {
//            LOGGER.info("Starting to merge " + sortedChunkPaths.size() + " sorted chunks.");

            CSVReader reader = new CSVReader(new FileReader(path.toFile()));
            String[] nextLine = reader.readNext();
            if (nextLine != null) {
                queue.add(new CSVRecord(nextLine, reader));
                openReaders.put(reader, nextLine);
            }
        }

        try (CSVWriter writer = new CSVWriter(new FileWriter(outputPath))) {
            while (!queue.isEmpty()) {
                CSVRecord record = queue.poll();
                writer.writeNext(record.line);
                String[] nextLine = record.reader.readNext();
                if (nextLine != null) {
                    queue.add(new CSVRecord(nextLine, record.reader));
                } else {
                    openReaders.remove(record.reader);
                    record.reader.close();
                }

            }
        }

        for (CSVReader reader : openReaders.keySet()) {
            reader.close();
        }
    }

    private static class CSVRecord implements Comparable<CSVRecord> {


        String[] line;
        CSVReader reader;

        CSVRecord(String[] line, CSVReader reader) {
            this.line = line;
            this.reader = reader;
        }

        @Override
        public int compareTo(CSVRecord other) {
            return this.line[1].compareTo(other.line[1]);
        }
    }
}
