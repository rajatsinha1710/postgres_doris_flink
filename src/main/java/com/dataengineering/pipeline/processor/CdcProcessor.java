package com.dataengineering.pipeline.processor;

import com.dataengineering.pipeline.model.CdcEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Processes CDC events from Kafka and maps them to Doris table columns.
 * No merging or enrichment - direct mapping based on source table.
 */
public class CdcProcessor implements MapFunction<String, Map<String, Object>> {
    
    private static final Logger logger = LoggerFactory.getLogger(CdcProcessor.class);
    
    @Override
    public Map<String, Object> map(String kafkaMessage) throws Exception {
        try {
            // Parse CDC event from Kafka message
            CdcEvent cdcEvent = CdcEvent.fromJson(kafkaMessage);
            
            if (cdcEvent == null) {
                logger.warn("Failed to parse CDC event from message: {}", kafkaMessage);
                return null;
            }
            
            // Skip DELETE operations for now (can be handled later if needed)
            if ("d".equals(cdcEvent.getOperation()) || "DELETE".equals(cdcEvent.getOperation())) {
                logger.debug("Skipping DELETE operation for table: {}", cdcEvent.getTableName());
                return null;
            }
            
            // Get mapped columns for Doris
            Map<String, Object> dorisColumns = cdcEvent.getDorisColumns();
            
            if (dorisColumns.isEmpty()) {
                logger.warn("No columns to upsert for CDC event: {}", cdcEvent);
                return null;
            }
            
            logger.debug("Processed CDC event - Table: {}, Operation: {}, Columns: {}", 
                        cdcEvent.getTableName(), cdcEvent.getOperation(), dorisColumns.keySet());
            
            return dorisColumns;
            
        } catch (Exception e) {
            logger.error("Failed to process CDC event: {}", kafkaMessage, e);
            return null;
        }
    }
}
