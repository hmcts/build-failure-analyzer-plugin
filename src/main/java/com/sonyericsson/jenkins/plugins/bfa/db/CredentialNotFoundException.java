package com.sonyericsson.jenkins.plugins.bfa.db;

/**
 * Indicates the credential configured in global config couldn't be found
 * (probably deleted)
 */
public class CredentialNotFoundException extends RuntimeException {

    public CredentialNotFoundException(String credentialId) {
        super(String.format("Credential with id %s not found, maybe it has been deleted", credentialId));
    }
}
