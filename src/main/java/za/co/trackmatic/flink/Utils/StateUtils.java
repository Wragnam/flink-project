package za.co.trackmatic.flink.Utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.time.Time;

/**
 * Utility class for working with Flink state, including state creation with TTL (time-to-live).
 */
public class StateUtils {

    /**
     * Creates a {@link MapState} with a configured TTL (time-to-live) setting.
     * <p>
     * The TTL configuration ensures state entries expire after the specified duration, with incremental cleanup.
     * The TTL is refreshed on state creation and write operations.
     *
     * @param <K>            The type of the state key.
     * @param <V>            The type of the state value.
     * @param ctx            The Flink runtime context to create state.
     * @param stateName      The name of the state.
     * @param ttl            The TTL duration for state entries.
     * @param keyType        The type information for the key.
     * @param valueTypeHint  The type hint for the value type.
     * @param cleanupSize    The approximate number of entries to clean up incrementally.
     * @return A MapState instance with the configured TTL.
     */
    public static <K, V> MapState<K, V> createMapStateWithTTL(
            RuntimeContext ctx,
            String stateName,
            Time ttl,
            TypeInformation<K> keyType,
            TypeHint<V> valueTypeHint,
            Integer cleanupSize
    ) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(cleanupSize, true)
                .build();

        MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(
                stateName,
                keyType,
                TypeInformation.of(valueTypeHint)
        );
        descriptor.enableTimeToLive(ttlConfig);

        return ctx.getMapState(descriptor);
    }
}
