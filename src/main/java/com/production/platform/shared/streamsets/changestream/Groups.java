package com.production.platform.shared.streamsets.changestream;

import com.streamsets.pipeline.api.Label;

import java.util.HashMap;
import java.util.Map;

public class Groups {
    public static final String ChangeStream = "ChangeStream";
    public static final String Security = "Security";


    interface CustomLabel<T extends Enum> extends Label {
        Map<String, String> labels = new HashMap<>();

        default void addLabel(String name, String value) {
            labels.put(name, value);
        }

        @Override
        default String getLabel() {
            return labels.get(((T) this).name());
        }
    }

    /**
     * Change stream group
     */
    public enum ChangeStreamGroup implements CustomLabel<ChangeStreamGroup> {
        ChangeStream(Groups.ChangeStream),
        Security(Groups.Security);

        ChangeStreamGroup(String label) {
            addLabel(this.name(), label);
        }
    }
}