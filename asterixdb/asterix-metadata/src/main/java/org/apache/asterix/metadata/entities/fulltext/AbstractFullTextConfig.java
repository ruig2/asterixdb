package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.asterix.fuzzyjoin.tokenizer.TokenizerFactory;
import org.apache.asterix.metadata.api.IFullTextConfig;
import org.apache.asterix.metadata.api.IFullTextFilter;
import org.apache.asterix.metadata.entities.Index;

import java.util.List;

public abstract class AbstractFullTextConfig implements IFullTextConfig {
    private final String name;
    private final Tokenizer tokenizer;
    private ImmutableList<IFullTextFilter> filters;
    private List<Index> usedByIndices;

    protected AbstractFullTextConfig(String name, Tokenizer tokenizer, ImmutableList<IFullTextFilter> filters) {
        this.name = name;
        this.tokenizer = tokenizer;
        this.filters = filters;
    }

    @Override public String getName() {
        return name;
    }

    @Override public FullTextEntityCategory getCategory() {
        return FullTextEntityCategory.CONFIG;
    }

    @Override public Tokenizer getTokenizer() {
        return tokenizer;
    }

    @Override public List<IFullTextFilter> getFilters() {
        return filters;
    }

    @Override public List<Index> getUsedByIndices() {
        return usedByIndices;
    }
}
