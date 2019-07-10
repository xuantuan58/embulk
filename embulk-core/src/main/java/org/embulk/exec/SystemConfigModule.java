package org.embulk.exec;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.embulk.SystemConfigProperties;
import org.embulk.config.ConfigSource;

public class SystemConfigModule implements Module {
    private final ConfigSource systemConfig;
    private final SystemConfigProperties systemConfigProperties;

    public SystemConfigModule(final ConfigSource systemConfig, final SystemConfigProperties systemConfigProperties) {
        this.systemConfig = systemConfig;
        this.systemConfigProperties = systemConfigProperties;
    }

    @SuppressWarnings("deprecation")  // Using ForSystemConfig
    @Override
    public void configure(Binder binder) {
        binder.bind(ConfigSource.class)
                .annotatedWith(ForSystemConfig.class)
                .toInstance(systemConfig);
        binder.bind(SystemConfigProperties.class)
                .toInstance(systemConfigProperties);
    }
}
