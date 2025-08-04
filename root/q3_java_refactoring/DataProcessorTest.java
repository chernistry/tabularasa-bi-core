package com.tabularasa.bi.q3_java_refactoring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DataProcessorTest {

    private DataProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new DataProcessor();
    }

    @Test
    void processBatch_shouldIdentifyDuplicatesAndStoreRecords() {
        List<Map<String, String>> batch = new ArrayList<>();
        batch.add(Map.of("id", "101", "type", "A", "data", "value1"));
        batch.add(Map.of("id", "102", "type", "B", "data", "value2"));
        batch.add(Map.of("id", "101", "type", "A", "data", "value3")); // Duplicate key

        List<String> duplicates = processor.processBatch(batch);

        assertEquals(1, duplicates.size(), "Should identify one duplicate key.");
        assertEquals("101:A", duplicates.get(0), "Duplicate key should be '101:A'.");

        Map<String, Long> counts = processor.getAllRecordCounts();
        assertEquals(2, counts.get("101:A"), "Count for '101:A' should be 2.");
        assertEquals(1, counts.get("102:B"), "Count for '102:B' should be 1.");
        assertEquals(2, processor.getUniqueRecordCount(), "Unique record count should be 2.");
    }

    @Test
    void processBatch_withEmptyBatch_shouldReturnEmptyDuplicatesList() {
        List<Map<String, String>> batch = new ArrayList<>();
        List<String> duplicates = processor.processBatch(batch);
        assertTrue(duplicates.isEmpty(), "Duplicates list should be empty for an empty batch.");
        assertEquals(0, processor.getUniqueRecordCount(), "Unique record count should be 0.");
    }

    @Test
    void processBatch_withNoDuplicates_shouldReturnEmptyDuplicatesList() {
        List<Map<String, String>> batch = new ArrayList<>();
        batch.add(Map.of("id", "101", "type", "A", "data", "value1"));
        batch.add(Map.of("id", "102", "type", "B", "data", "value2"));
        List<String> duplicates = processor.processBatch(batch);
        assertTrue(duplicates.isEmpty(), "Duplicates list should be empty when no duplicates exist.");
        assertEquals(2, processor.getUniqueRecordCount(), "Unique record count should be 2.");
    }

    @Test
    void processBatch_multipleBatches_shouldAccumulateCountsAndDuplicates() {
        List<Map<String, String>> batch1 = new ArrayList<>();
        batch1.add(Map.of("id", "S01", "type", "X", "value", "abc"));
        batch1.add(Map.of("id", "S02", "type", "Y", "value", "def"));
        processor.processBatch(batch1);

        List<Map<String, String>> batch2 = new ArrayList<>();
        batch2.add(Map.of("id", "S01", "type", "X", "value", "ghi")); // Duplicate from batch1
        batch2.add(Map.of("id", "S03", "type", "Z", "value", "jkl"));
        List<String> duplicatesBatch2 = processor.processBatch(batch2);

        assertEquals(1, duplicatesBatch2.size());
        assertEquals("S01:X", duplicatesBatch2.get(0));

        assertEquals(3, processor.getUniqueRecordCount(), "Unique record count should be 3 after two batches.");
        Map<String, Long> counts = processor.getAllRecordCounts();
        assertEquals(2, counts.get("S01:X"), "Count for S01:X should be 2.");
        assertEquals(1, counts.get("S02:Y"), "Count for S02:Y should be 1.");
        assertEquals(1, counts.get("S03:Z"), "Count for S03:Z should be 1.");
    }

    @Test
    void getAllRecordCounts_onNewInstance_shouldReturnEmptyMap() {
        DataProcessor newProcessor = new DataProcessor();
        assertTrue(newProcessor.getAllRecordCounts().isEmpty(), "getAllRecordCounts should return an empty map for a new instance.");
    }

    @Test
    void getUniqueRecordCount_onNewInstance_shouldReturnZero() {
        DataProcessor newProcessor = new DataProcessor();
        assertEquals(0, newProcessor.getUniqueRecordCount(), "getUniqueRecordCount should return 0 for a new instance.");
    }
} 