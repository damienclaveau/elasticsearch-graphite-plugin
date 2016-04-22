package org.elasticsearch.plugin.graphite;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.service.graphite.GraphiteService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesModule;

import java.util.ArrayList;
import java.util.Collection;

public class GraphitePlugin extends Plugin {

    private final Settings settings;

    public GraphitePlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "graphite";
    }

    public String description() {
        return "Graphite Monitoring Plugin";
    }

    @SuppressWarnings("rawtypes")
    @Override public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(GraphiteService.class);
        return services;
    }

    @Override
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    public void onModule(RepositoriesModule repositoriesModule) {
    }
    

}
