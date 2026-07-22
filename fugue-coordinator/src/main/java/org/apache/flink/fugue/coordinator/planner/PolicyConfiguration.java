/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fugue.coordinator.planner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for migration behavior: the pre-copy round bound, the network-bandwidth hint, and the
 * {@code migration.*} timeout keys read by {@link
 * org.apache.flink.fugue.coordinator.manager.MigrationManager}. Values are a typed key/value map so
 * callers can supply arbitrary keys with defaults.
 */
public class PolicyConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Maximum number of pre-copy rounds before a non-converging migration is aborted. */
    public static final String MAX_PRECOPY_ROUNDS = "max.precopy.rounds";

    /** Advisory pre-copy bandwidth limit, in Mbps (passed to the transfer; see {@code transferSnapshot}). */
    public static final String NETWORK_BANDWIDTH_LIMIT = "network.bandwidth.limit.mbps";

    private final Map<String, Object> parameters;

    public PolicyConfiguration() {
        this.parameters = new HashMap<>();
        setDefaults();
    }

    /**
     * Set default configuration values.
     */
    private void setDefaults() {
        parameters.put(MAX_PRECOPY_ROUNDS, 10);
        parameters.put(NETWORK_BANDWIDTH_LIMIT, 100.0);         // 100 Mbps
    }

    /**
     * Get a configuration parameter.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) parameters.get(key);
    }

    /**
     * Set a configuration parameter.
     */
    public void set(String key, Object value) {
        parameters.put(key, value);
    }

    /**
     * Get a double parameter with default value.
     */
    public double getDouble(String key, double defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }

    /**
     * Get an integer parameter with default value.
     */
    public int getInt(String key, int defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    /**
     * Get a long parameter with default value.
     */
    public long getLong(String key, long defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return defaultValue;
    }

    /**
     * Get a boolean parameter with default value.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }

    /**
     * Check if a key exists.
     */
    public boolean contains(String key) {
        return parameters.containsKey(key);
    }

    /**
     * Get all parameters.
     */
    public Map<String, Object> getAllParameters() {
        return new HashMap<>(parameters);
    }

    /**
     * Create a copy of this configuration.
     */
    public PolicyConfiguration copy() {
        PolicyConfiguration copy = new PolicyConfiguration();
        copy.parameters.clear();
        copy.parameters.putAll(this.parameters);
        return copy;
    }

    @Override
    public String toString() {
        return "PolicyConfiguration{" + parameters + "}";
    }
}