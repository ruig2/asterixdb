package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.asterix.metadata.api.IFullTextFilter;

public class FullTextConfig extends AbstractFullTextConfig {
    protected FullTextConfig(String name, Tokenizer tokenizer, ImmutableList<IFullTextFilter> filters) {
        super(name, tokenizer, filters);
    }


}
