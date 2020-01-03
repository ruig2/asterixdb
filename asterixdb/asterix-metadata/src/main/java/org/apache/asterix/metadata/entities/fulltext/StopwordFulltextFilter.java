package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class StopwordFulltextFilter extends AbstractFulltextFilter {
    ImmutableList<String> stopwordList;

    @Override public FulltextFilterCategory getFilterCategory() {
        return FulltextFilterCategory.STOPWORD;
    }

    public StopwordFulltextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, FulltextFilterCategory.STOPWORD);
        this.stopwordList = stopwordList;
    }

    public List<String> getStopwordList() {
        return stopwordList;
    }
}
