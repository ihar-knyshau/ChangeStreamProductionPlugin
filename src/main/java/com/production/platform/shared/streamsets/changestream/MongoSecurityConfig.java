package com.production.platform.shared.streamsets.changestream;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class MongoSecurityConfig {

    @ConfigDef(
            type = ConfigDef.Type.BOOLEAN,
            label = "Enable Authentication",
            defaultValue = "false",
            displayPosition = 40,
            required = true,
            group = Groups.Security
    )
    public boolean enableAuthentication;

    @ConfigDef(
            type = ConfigDef.Type.CREDENTIAL,
            label = "Username",
            required = false,
            dependsOn = "enableAuthentication",
            displayPosition = 40,
            triggeredByValue = {"true"},
            group = Groups.Security
    )
    public CredentialValue username;

    @ConfigDef(
            type = ConfigDef.Type.CREDENTIAL,
            label = "Password",
            required = false,
            dependsOn = "enableAuthentication",
            displayPosition = 40,
            triggeredByValue = {"true"},
            group = Groups.Security
    )
    public CredentialValue password;

    @ConfigDef(
            type = ConfigDef.Type.CREDENTIAL,
            label = "Source DB",
            defaultValue = "admin",
            required = false,
            dependsOn = "enableAuthentication",
            displayPosition = 40,
            triggeredByValue = {"true"},
            group = Groups.Security
    )
    public CredentialValue sourceDB;

    public boolean isEnableAuthentication() {
        return enableAuthentication;
    }

    public CredentialValue getUsername() {
        return username;
    }

    public CredentialValue getPassword() {
        return password;
    }

    public CredentialValue getSourceDB() {
        return sourceDB;
    }
}
