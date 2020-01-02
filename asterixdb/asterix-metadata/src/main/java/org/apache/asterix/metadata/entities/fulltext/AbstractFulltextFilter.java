package org.apache.asterix.metadata.entities.fulltext;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.metadata.api.IFulltextFilter;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

import java.util.List;

public abstract class AbstractFulltextFilter implements IFulltextFilter {
    private String name = null;
    private FulltextFilterType type;
    private List<String> usedByFTConfigs;

    public AbstractFulltextFilter(String name, FulltextFilterType type) {
        this.name = name;
        this.type = type;
    }

    @Override public String getName() {
        return name;
    }

    @Override public FulltextFilterType getType() {
        return type;
    }

    @Override public List<String> getUsedByFTConfigs() {
        return usedByFTConfigs;
    }

    @Override public void addUsedByFTConfigs(String ftConfigName) {
        usedByFTConfigs.add(ftConfigName);
    }
}
