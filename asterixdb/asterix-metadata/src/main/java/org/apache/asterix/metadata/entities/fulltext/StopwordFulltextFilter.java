package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;

public class StopwordFulltextFilter extends AbstractFulltextFilter {
    ImmutableList<String> stopwordList;

    @Override public FulltextFilterType getType() {
        return FulltextFilterType.Stopword;
    }

    public StopwordFulltextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, FulltextFilterType.Stopword);
        this.stopwordList = stopwordList;
    }
}
