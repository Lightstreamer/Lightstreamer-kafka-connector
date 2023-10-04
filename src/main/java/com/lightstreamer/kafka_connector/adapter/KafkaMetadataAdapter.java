package com.lightstreamer.kafka_connector.adapter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.lightstreamer.interfaces.metadata.ItemsException;
import com.lightstreamer.interfaces.metadata.MetadataProviderAdapter;
import com.lightstreamer.interfaces.metadata.SchemaException;

public class KafkaMetadataAdapter extends MetadataProviderAdapter{

    @Override
    @Nonnull
    public String[] getItems(@Nullable String user, @Nonnull String sessionID, @Nonnull String group,
            @Nonnull String dataAdapter) throws ItemsException {
        throw new UnsupportedOperationException("Unimplemented method 'getItems'");
    }

    @Override
    @Nonnull
    public String[] getSchema(@Nullable String user, @Nonnull String sessionID, @Nonnull String group,
            @Nonnull String dataAdapter, @Nonnull String schema) throws ItemsException, SchemaException {
        throw new UnsupportedOperationException("Unimplemented method 'getSchema'");
    }
    
}
