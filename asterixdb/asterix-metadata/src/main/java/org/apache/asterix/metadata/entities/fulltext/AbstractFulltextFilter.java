package org.apache.asterix.metadata.entities.fulltext;

import org.apache.asterix.metadata.api.IFulltextFilter;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFulltextFilter implements IFulltextFilter {
    private String name = null;
    private FulltextFilterKind type;
    private List<String> usedByFTConfigs;

    public AbstractFulltextFilter(String name, FulltextFilterKind type) {
        this.name = name;
        this.type = type;
        this.usedByFTConfigs = new ArrayList<>();
    }

    @Override
    public FulltextEntityCategory getCategory() {
        return FulltextEntityCategory.FULLTEXT_FILTER;
    }

    @Override public String getName() {
        return name;
    }

    @Override
    public FulltextFilterKind getFilterKind() {
        return type;
    }

    @Override public List<String> getUsedByFTConfigs() {
        return usedByFTConfigs;
    }

    @Override public void addUsedByFTConfigs(String ftConfigName) {
        usedByFTConfigs.add(ftConfigName);
    }
}
