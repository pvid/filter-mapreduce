package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.Serializable;
import java.util.function.Predicate;

@FunctionalInterface
public interface SerializablePredicate<T> extends Serializable, Predicate<T> {
}
