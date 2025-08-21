package org.reactome.release.cosmicupdate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.InstanceDisplayNameGenerator;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidAttributeException;
import org.gk.schema.InvalidAttributeValueException;
import org.reactome.release.common.database.InstanceEditUtils;

/**
 * Updates a COSMIC Identifier.
 * Implements <code>Comparable</code> to make sorting easier for reporting purposes.
 * Does *not* override the <code>equals</code> method.
 * @author sshorser
 *
 */
public class COSMICIdentifierUpdater implements Comparable<COSMICIdentifierUpdater> {
	private static final Logger logger = LogManager.getLogger();

	private String identifier;
	private GKInstance cosmicDatabaseIdentifierInstance;
	private String suggestedPrefix;
	private boolean valid;
	private Set<String> mutationIDs = new HashSet<>();
	private String cosvIdentifier;

	public String getIdentifier() {
		return this.identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public GKInstance getCosmicDatabaseIdentifierInstance() {
		return this.cosmicDatabaseIdentifierInstance;
	}

	public void setCosmicDatabaseIdentifierInstance(GKInstance cosmicDatabaseIdentifierInstance) {
		this.cosmicDatabaseIdentifierInstance = cosmicDatabaseIdentifierInstance;
	}

	public String getSuggestedPrefix() {
		return this.suggestedPrefix;
	}

	public void setSuggestedPrefix(String suggestedPrefix) {
		this.suggestedPrefix = suggestedPrefix;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public Set<String> getMutationIDs() {
		return this.mutationIDs;
	}

	public void addMutationID(String mutationID) {
		this.mutationIDs.add(mutationID);
	}

	public String getCosvIdentifier() {
		return this.cosvIdentifier;
	}

	public void setCosvIdentifier(String cosvIdentifier) {
		this.cosvIdentifier = cosvIdentifier;
	}
	
	@Override
	public String toString() {
		return "[" + this.getCosmicDatabaseIdentifierInstance() + "; "
					+ this.getIdentifier()+ "; "
					+ this.getSuggestedPrefix() + "; "
					+ this.isValid() + "; "
					+ this.getCosvIdentifier() + "; ("
					+ String.join(",", this.getMutationIDs()) + ")" + "]";
	}
	
	/**
	 * Implementation of compareTo ensures that these are sorted by validity, and then by identifier.
	 * This is done so that the invalid identifiers are at the top of the report.
	 */
	@Override
	public int compareTo(COSMICIdentifierUpdater other) {
		if (other.valid && !this.valid) {
			return -1;
		}
		if (!other.valid && this.valid) {
			return 1;
		}
        return this.identifier.compareTo(other.identifier);
    }

	/**
	 * Perform an update of a COSMIC identifier.
	 * @param modifiedInstanceEdit
	 * @throws Exception
	 */
	public void updateIdentifier(GKInstance modifiedInstanceEdit) throws Exception {
		// If there is a COSV identifier, we'll update using that.
		if (cosvIdentifierExists()) {
			updateUsingCOSVIdentifier(modifiedInstanceEdit);
		}
		// If no COSV identifier was found, update using the suggested prefix (determined computationally).
		else if (suggestedPrefixIsCOSMICLegacyPrefix()) {
			updateUsingSuggestedCOSMICPrefix(modifiedInstanceEdit);
		}
		// Some identifiers won't have a COSV identifier in the COSMIC files, and they might not have a suggested
		// prefix either.
		else {
			logger.info(
				"No suggested prefix OR COSV identifier for {} (DBID: {}) - identifier will not be updated.",
				this.getIdentifier(), this.getDbID());
		}
	}

	private boolean cosvIdentifierExists() {
		return this.getCosvIdentifier() != null && !this.getCosvIdentifier().isEmpty();
	}

	private boolean suggestedPrefixIsCOSMICLegacyPrefix() {
		return this.getSuggestedPrefix() != null &&
			this.getSuggestedPrefix().equalsIgnoreCase(COSMICUpdateUtil.COSMIC_LEGACY_PREFIX);
	}

	private void updateUsingCOSVIdentifier(GKInstance modifiedInstanceEdit) throws Exception {
		updateCOSMICDatabaseIdentifierInstance(this.getCosvIdentifier(), modifiedInstanceEdit);
	}

	private void updateUsingSuggestedCOSMICPrefix(GKInstance modifiedInstanceEdit) throws Exception {
		GKInstance cosmicDatabaseIdentifierInstance = this.getCosmicDatabaseIdentifierInstance();
		String currentCOSMICIdentifier = (String)
			cosmicDatabaseIdentifierInstance.getAttributeValue(ReactomeJavaConstants.identifier);
		// If the current identifier already begins with "C" then leave it alone.
		// This code is for updating numeric identifiers that have a suggested prefix.
		if (!COSMICUpdateUtil.stringStartsWithC(currentCOSMICIdentifier.toUpperCase())) {
			String newCOSMICIdentifier = this.getSuggestedPrefix() + currentCOSMICIdentifier;
			updateCOSMICDatabaseIdentifierInstance(newCOSMICIdentifier, modifiedInstanceEdit);
		}
	}
	
	/**
	 * Executes an update on an instance.
	 * Sets the identifier attribute of <code>identifierObject</code> to the value of <code>identifierValue</code>.
	 * <code>identifierObject</code> (which must be an InstanceEdit) will also have
	 * <code>modifiedForCOSMICUpdate</code> added to its <code>modified</code> list.
	 * The display name of <code>identifierObject</code> will also be regenerated to reflect changes in
	 * <code>identifierValue</code>.
	 * @param identifierValue An identifier value that will be set on <code>identifierObject</code>
	 * @param modifiedInstanceEdit An InstanceEdit which explains why an instance was modified.
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws InvalidAttributeValueException
	 */
	private void updateCOSMICDatabaseIdentifierInstance(
		String identifierValue, GKInstance modifiedInstanceEdit
	) throws InvalidAttributeException, Exception, InvalidAttributeValueException {
		GKInstance cosmicDatabaseIdentifierInstance = getCosmicDatabaseIdentifierInstance();
		// Set the identifier value.
		cosmicDatabaseIdentifierInstance.setAttributeValue(ReactomeJavaConstants.identifier, identifierValue);
		
		// Add the instance edit to the modified list
		List<GKInstance> modifications =
			(List<GKInstance>) cosmicDatabaseIdentifierInstance.getAttributeValuesList(ReactomeJavaConstants.modified);
		modifications.add(modifiedInstanceEdit);
		cosmicDatabaseIdentifierInstance.setAttributeValue(ReactomeJavaConstants.modified, modifications);
		
		// Update the displayname after other changes (setDisplayName will generate a new value and then set it)
		InstanceDisplayNameGenerator.setDisplayName(cosmicDatabaseIdentifierInstance);

		MySQLAdaptor adaptor = (MySQLAdaptor) cosmicDatabaseIdentifierInstance.getDbAdaptor();
		adaptor.updateInstanceAttribute(cosmicDatabaseIdentifierInstance, ReactomeJavaConstants.identifier);
		adaptor.updateInstanceAttribute(cosmicDatabaseIdentifierInstance, ReactomeJavaConstants.modified);
		adaptor.updateInstanceAttribute(cosmicDatabaseIdentifierInstance, ReactomeJavaConstants._displayName);
	}

	private long getDbID() {
		return getCosmicDatabaseIdentifierInstance().getDBID();
	}
}
