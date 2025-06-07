import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

/**
 * Processes records, identifies duplicates within a batch, and counts all processed records.
 */
public class DataProcessor {
    private final Set<String> processedRecordKeys = new HashSet<>();
    private final Map<String, Integer> recordCounts = new HashMap<>();

    /**
     * Processes a batch of records. A record is considered a duplicate
     * if its generated key has been seen in any previous or current batch processing.
     *
     * @param batch A list of records, where each record is a Map of string key-value pairs.
     *              Each record is expected to contain "id" and "type" keys.
     * @return A list of keys that were duplicates in the context of all processed records.
     */
    public List<String> processBatch(List<Map<String, String>> batch) {
        if (batch == null) {
            return Collections.emptyList();
        }
        List<String> duplicatesInBatch = new ArrayList<>();
        for (Map<String, String> record : batch) {
            if (record == null || !record.containsKey("id") || !record.containsKey("type")) {
                continue;
            }
            String key = String.join(":", record.get("id"), record.get("type"));
            recordCounts.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
            if (!processedRecordKeys.add(key)) {
                duplicatesInBatch.add(key);
            }
        }
        return duplicatesInBatch;
    }

    /**
     * Returns an unmodifiable view of the record counts map.
     *
     * @return An unmodifiable map of record keys to their counts.
     */
    public Map<String, Integer> getAllRecordCounts() {
        return Collections.unmodifiableMap(recordCounts);
    }

    /**
     * Returns the count of unique records processed so far.
     *
     * @return The number of unique record keys encountered.
     */
    public long getUniqueRecordCount() {
        return processedRecordKeys.size();
    }

    public static void main(String[] args) {
        DataProcessor processor = new DataProcessor();
        List<Map<String, String>> batch1 = new ArrayList<>();
        batch1.add(Map.of("id", "101", "type", "A"));
        batch1.add(Map.of("id", "102", "type", "B"));
        batch1.add(Map.of("id", "101", "type", "A"));
        List<String> dupes = processor.processBatch(batch1);
        System.out.println("Duplicates in batch1: " + dupes);
        System.out.println("Unique records: " + processor.getUniqueRecordCount());
        System.out.println("Counts: " + processor.getAllRecordCounts());
        List<Map<String, String>> batch2 = new ArrayList<>();
        batch2.add(Map.of("id", "103", "type", "C"));
        batch2.add(Map.of("id", "101", "type", "A"));
        List<String> dupes2 = processor.processBatch(batch2);
        System.out.println("Duplicates in batch2: " + dupes2);
        System.out.println("Unique records: " + processor.getUniqueRecordCount());
        System.out.println("Counts: " + processor.getAllRecordCounts());
    }
} 