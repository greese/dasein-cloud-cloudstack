package org.dasein.cloud.cloudstack.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.compute.SnapshotCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudstack with respect to Dasein snapshot operations.
 * User: daniellemayne
 * Date: 05/03/2014
 * Time: 15:39
 */
public class CSSnapshotCapabilities extends AbstractCapabilities<CSCloud> implements SnapshotCapabilities{
    public CSSnapshotCapabilities(@Nonnull CSCloud provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Nonnull
    @Override
    public Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean supportsSnapshotCopying() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return false;
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return false;
    }
}
