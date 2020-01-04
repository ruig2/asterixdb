package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class StopwordFulltextFilter extends AbstractFulltextFilter {
    ImmutableList<String> stopwordList;

    @Override public FulltextFilterKind getFilterKind() {
        return FulltextFilterKind.STOPWORD;
    }

    public StopwordFulltextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, FulltextFilterKind.STOPWORD);
        this.stopwordList = stopwordList;
    }

    public List<String> getStopwordList() {
        return stopwordList;
    }
}
