package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;

public class StopwordFulltextFilter extends AbstractFulltextFilter {
    ImmutableList<String> stopwordList;

    @Override public FulltextFilterType getType() {
        return FulltextFilterType.STOPWORD;
    }

    public StopwordFulltextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, FulltextFilterType.STOPWORD);
        this.stopwordList = stopwordList;
    }
}
