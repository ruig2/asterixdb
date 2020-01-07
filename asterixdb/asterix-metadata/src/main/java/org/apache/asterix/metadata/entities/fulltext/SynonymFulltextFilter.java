package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SynonymFulltextFilter extends AbstractFulltextFilter {
    ImmutableMap<String, String> synonymMap;

    public SynonymFulltextFilter(String name, ImmutableMap<String, String> synonymMap) {
        super(name, FulltextFilterKind.STOPWORD);
        this.synonymMap = synonymMap;
    }
}
