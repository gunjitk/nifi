package org.apache.nifi.edn;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.*;
import us.bpsm.edn.EdnException;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class EdnRecordReader extends AbstractEdnRowRecordReader {

    private List<RecordField> recordFields;
    private final Scanner scanner;
    private final Parser parser;

    EdnRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding)
        throws MalformedRecordException {
        super(logger, schema, dateFormat, timeFormat, timestampFormat);

        try {
            scanner = new Scanner(in, encoding);
            parser = Parsers.newParser(Parsers.defaultConfiguration());

        } catch (EdnException ex) {
            throw new MalformedRecordException("Could not parse data as EDN", ex);
        }
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {

        try {
            final RecordSchema schema = getSchema();
            final List<RecordField> recordFields = getRecordFields();
            scanner.useDelimiter("\n"); // Can be configured made a configurable property..

            while (scanner.hasNext()) {
                String rawValue;
                String rawFieldName;
                DataType dataType;
                final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
                final String nextRecord = scanner.next();
                this.logger.log(LogLevel.INFO, nextRecord);
                Parseable pbr = Parsers.newParseable(nextRecord);
                Map<?, ?> recordMap = (Map<?,?>) parser.nextValue(pbr);

                for(Map.Entry<?,?> entry: recordMap.entrySet()) {
                    final RecordField rawField = recordFields.stream()
                            .filter(item -> item.getFieldName().equals(entry.getKey().toString().substring(1)))
                            .collect(Collectors.toList()).get(0);
                    rawFieldName = rawField.getFieldName();
                    dataType = rawField.getDataType();
                    try {
                        rawValue = entry.getValue().toString();
                    } catch (NullPointerException ex) {
                        this.logger.log(LogLevel.DEBUG, "Got Null Value while parsing edn string for field" +  rawFieldName);
                        rawValue = "NIL";
                    }
                    final Object value;
                    if (coerceTypes) {
                        value = convert(rawValue, dataType, rawFieldName);
                    } else {
                        value = convertSimpleIfPossible(rawValue, dataType, rawFieldName);
                    }

                    values.put(rawFieldName, value);
                }
                return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
            }

        } catch (Exception ex) {
            throw new MalformedRecordException("Error occured while processing the record, Root Cause", ex);
        }
        return null;
    }

    private List<RecordField> getRecordFields() {
        return this.schema.getFields();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }
}
