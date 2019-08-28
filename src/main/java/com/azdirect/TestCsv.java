package com.azdirect;

import com.talend.csv.CSVReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TestCsv {

    public void run() throws Exception {

        AtomicLong count = new AtomicLong();
        streamCsv("E:\\test2.csv")
                .skip(1)
                .forEach(values -> {
                    if (count.incrementAndGet() % 1000000 == 0)
                        System.out.println("Row " + Arrays.toString(values));
                });


    }

    private Stream<String[]> streamCsv(String filePath) throws Exception {
        final CSVReader cr = new CSVReader(filePath, ';', "utf-8");
        cr.setQuoteChar('"');
        cr.setEscapeChar('\0');
        cr.setTrimWhitespace(true);
        cr.setSkipEmptyRecords(true);
        return StreamSupport.stream(new CsvSpliterator(cr), false).onClose(() -> {
            try { cr.close(); } catch (IOException e) { throw new UncheckedIOException(e); }
        });

    }

    public static void main(String[] args) throws Exception {
        TestCsv testCsv = new TestCsv();
        testCsv.run();

    }
}
