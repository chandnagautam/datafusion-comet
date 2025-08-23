package org.apache.comet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class JniStore {
    private Map<String, Object> objectStore;

    private JniStore() {
        this.objectStore = new ConcurrentHashMap<>();
    }

    private static class JniStoreHolder {
        static final JniStore INSTANCE = new JniStore();
    }

    public static void add(String key, Object obj) {
        JniStoreHolder.INSTANCE.objectStore.put(key, obj);
    }

    public static Object get(String key) {
        return JniStoreHolder.INSTANCE.objectStore.get(key);
    }
}
