package com.rackspacecloud.metrics.rollup.domain;

public interface IReducer<T> {
    String getRollupKey();
    T reduce(T newValue);
}
