package dev.vidlicka.hbase.filtermapreduce.reducer;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {
}
