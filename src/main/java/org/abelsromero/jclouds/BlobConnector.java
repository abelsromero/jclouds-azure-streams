package org.abelsromero.jclouds;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.netty.config.NettyPayloadModule;

import java.util.Properties;

import static org.jclouds.Constants.*;

public class BlobConnector {

    public static final String PROVIDER_OR_API = "azureblob";

    private final String storageAccountName;
    private final String storageAccountKey;


    public BlobConnector(final String storageAccountName, final String storageAccountKey) {
        this.storageAccountName = storageAccountName;
        this.storageAccountKey = storageAccountKey;
    }

    public BlobStoreContext openBlobStoreContext() {
        return openBlobStoreContext(33_554_432l);
    }

    /**
     * @param maxPartSize Nullable, 33_554_432 (32MB by default)
     */
    public BlobStoreContext openBlobStoreContext(final Long maxPartSize) {

        final Properties overrides = new Properties();
        overrides.setProperty(PROPERTY_MAX_CONNECTIONS_PER_CONTEXT, "4");
        overrides.setProperty(PROPERTY_MAX_CONNECTIONS_PER_HOST, "4");
        final String timeout = Long.toString(30000l);
        overrides.setProperty(PROPERTY_CONNECTION_TIMEOUT, timeout);
        overrides.setProperty(PROPERTY_SO_TIMEOUT, timeout);
        overrides.setProperty(PROPERTY_REQUEST_TIMEOUT, timeout);
        // multipart parallel upload strategy -> https://jclouds.apache.org/start/blobstore/
        overrides.setProperty("jclouds.mpu.parallel.degree", "4"); // default is 4
        overrides.setProperty("jclouds.mpu.parts.size", Long.toString(maxPartSize == null ? 33_554_432l : maxPartSize)); // 32 MB (default is 32 MB)

        final Iterable<Module> wiring = ImmutableSet.of(new NettyPayloadModule(), new SLF4JLoggingModule());

        return ContextBuilder.newBuilder(PROVIDER_OR_API)
            .credentials(storageAccountName, storageAccountKey)
            .modules(wiring)
            .overrides(overrides)
            .apiVersion("2016-05-31")
            .buildView(BlobStoreContext.class);
    }

}
