package com.example;

import com.example.CSVSorter;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.Test;
import java.util.logging.Logger;

public class CSVSorterTest {

    private static final Logger LOGGER = Logger.getLogger(CSVSorterTest.class.getName());

    @Test
    public void testDifferentMaxRowsValues() {
        int[] maxRowsValues = {2, 5, 10, 20}; // Different values to test
        String inputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\example_csv.csv";
        String outputPath = "C:\\Users\\RonS.MGMT\\IdeaProjects\\HomeTask\\sorting\\src\\main\\java\\com\\example\\a.csv";
//        String inputPath = "path_to_input.csv";
//        String outputPath = "path_to_output.csv";
        for (int maxRows : maxRowsValues) {
            CSVSorter sorter = new CSVSorter(maxRows);

            long startTime = System.currentTimeMillis();
            try {
                CSVSorter.main(new String[]{inputPath, outputPath}); // Simulate the main method call
            } catch (CsvValidationException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();

            LOGGER.info("Execution time with MAX_ROWS = " + maxRows + ": " + (endTime - startTime) + " milliseconds");
        }
    }
}