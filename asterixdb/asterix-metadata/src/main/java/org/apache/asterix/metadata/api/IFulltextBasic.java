package org.apache.asterix.metadata.api;

public interface IFulltextBasic {
    enum FulltextCategory {
        FULLTEXT_FILTER,
        FULLTEXT_CONFIG
    }

    FulltextCategory getCategory();
}
