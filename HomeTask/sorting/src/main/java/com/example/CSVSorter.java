package com.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import java.util.logging.Level;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Logger;

public class CSVSorter {
    private static final Logger LOGGER = Logger.getLogger(CSVSorter.class.getName());

//    private static final int MAX_ROWS = 10; // Maximum rows in memory

    private static int MAX_ROWS;
    public CSVSorter(int maxRows) {
        CSVSorter.MAX_ROWS = maxRows;
    }

    public static void main(String[] args) throws CsvValidationException {
        long startTime = System.currentTimeMillis();  // Start time
//        String inputPath = "path_to_input.csv";
//        String outputPath = "path_to_output.csv";
        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        String outputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\a1.csv";


        try {
            List<Path> sortedChunkPaths = splitAndSortChunks(inputPath);
            mergeSortedChunks(sortedChunkPaths, outputPath);
            // Clean up temporary files
            for (Path path : sortedChunkPaths) {
                Files.delete(path);
            }
            long endTime = System.currentTimeMillis();  // End time
            long duration = endTime - startTime;  // Calculate duration
            System.out.println("CSV sorted successfully.");
            LOGGER.info("Total execution time: " + (endTime - startTime) + " milliseconds");

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "An error occurred during the sorting process", e);
            e.printStackTrace();
        }
    }

    private static List<Path> splitAndSortChunks(String inputPath) throws IOException {
        List<Path> sortedChunkPaths = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(inputPath))) {
            List<String[]> rows = new ArrayList<>();
            String[] nextLine;
            while (true) {
                try {
                    if (!((nextLine = reader.readNext()) != null)) break;
                } catch (CsvValidationException e) {
                    throw new RuntimeException(e);
                }
                rows.add(nextLine);
                if (rows.size() == MAX_ROWS) {
                    sortedChunkPaths.add(sortAndWriteChunk(rows));
                    rows.clear();
                }
            }
            if (!rows.isEmpty()) {
                sortedChunkPaths.add(sortAndWriteChunk(rows));
            }
        }
        return sortedChunkPaths;
    }

    private static Path sortAndWriteChunk(List<String[]> chunk) throws IOException {
        chunk.sort(Comparator.comparing(row -> row[1]));
        Path chunkFile = Files.createTempFile("sortedChunk", ".csv");
        try (CSVWriter writer = new CSVWriter(new FileWriter(chunkFile.toFile()))) {
            for (String[] row : chunk) {
                writer.writeNext(row);
            }
        }
        return chunkFile;
    }

    private static void mergeSortedChunks(List<Path> sortedChunkPaths, String outputPath) throws IOException, CsvValidationException {
        PriorityQueue<CSVRecord> queue = new PriorityQueue<>();
        Map<CSVReader, String[]> openReaders = new HashMap<>();

        // Initialize readers for each chunk and load first line
        for (Path path : sortedChunkPaths) {
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
                }
            }
        }

        // Close all readers
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
