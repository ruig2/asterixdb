package org.apache.asterix.metadata.api;

import java.util.List;

// in progress...

public interface IFulltextFilter {
    enum FulltextFilterType {
        Stopword,
        Synonym
    }

    FulltextFilterType getType();
    String getName();
    List<String> getUsedByFTConfigs();
    void addUsedByFTConfigs(String ftConfigName);
}
