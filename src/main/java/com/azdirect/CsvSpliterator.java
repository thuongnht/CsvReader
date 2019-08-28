package com.azdirect;

import com.talend.csv.CSVReader;

import java.util.function.Consumer;


public class CsvSpliterator extends FixedBatchSpliteratorBase<String[]> {

    private final CSVReader cr;

    CsvSpliterator(CSVReader cr, int batchSize) {
        super(IMMUTABLE | ORDERED | NONNULL, batchSize);
        if (cr == null) throw new NullPointerException("CSVReader is null");
        this.cr = cr;
    }

    public CsvSpliterator(CSVReader cr) {
        this(cr, 128);
    }

    @Override
    public boolean tryAdvance(Consumer<? super String[]> action) {
        if (action == null) throw new NullPointerException();
        try {
            if (!cr.readNext()) return false;
            action.accept(cr.getValues());
            return true;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEachRemaining(Consumer<? super String[]> action) {
        if (action == null) throw new NullPointerException();
        try {
            for (String[] row; cr.readNext(); ) action.accept(cr.getValues());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

