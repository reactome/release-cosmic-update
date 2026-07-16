package org.reactome.release.cosmicupdate;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.ReactomeJavaConstants;
import org.reactome.curation.model.SimpleInstance;

import static org.reactome.release.cosmicupdate.COSMICUpdateUtil.getPersonId;

/**
 * Updates a COSMIC Identifier.
 * Implements <code>Comparable</code> to make sorting easier for reporting purposes.
 * Does *not* override the <code>equals</code> method.
 * @author sshorser
 *
 */
public class COSMICIdentifierUpdater implements Comparable<COSMICIdentifierUpdater> {
	private static final Logger logger = LogManager.getLogger();

	private CuratorToolAPI curatorToolAPI = new CuratorToolAPI();

	private String identifier;
	private SimpleInstance cosmicDatabaseIdentifierInstance;
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

	public SimpleInstance getCosmicDatabaseIdentifierInstance() {
		return this.cosmicDatabaseIdentifierInstance;
	}

	public void setCosmicDatabaseIdentifierInstance(SimpleInstance cosmicDatabaseIdentifierInstance) {
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
	 */
	public void updateIdentifier() {
		// If there is a COSV identifier, we'll update using that.
		if (cosvIdentifierExists()) {
			updateUsingCOSVIdentifier();
		}
		// If no COSV identifier was found, update using the suggested prefix (determined computationally).
		else if (suggestedPrefixIsCOSMICLegacyPrefix()) {
			updateUsingSuggestedCOSMICPrefix();
		}
		// Some identifiers won't have a COSV identifier in the COSMIC files, and they might not have a suggested
		// prefix either.
		else {
			logger.info(
				"No suggested prefix OR COSV identifier for {} (DBID: {}) - identifier will not be updated.",
				this.getIdentifier(), this.getDbID());
		}
	}

	String generateDisplayName(SimpleInstance cosmicDatabaseIdentifierInstance) {
		SimpleInstance referenceDatabase =
			(SimpleInstance) cosmicDatabaseIdentifierInstance.getAttribute(ReactomeJavaConstants.referenceDatabase);
		String referenceDatabaseName = referenceDatabase.getDisplayName();
		String identifier = (String) cosmicDatabaseIdentifierInstance.getAttribute(ReactomeJavaConstants.identifier);

		return referenceDatabaseName + ":" + identifier;
	}

	private boolean cosvIdentifierExists() {
		return this.getCosvIdentifier() != null && !this.getCosvIdentifier().isEmpty();
	}

	private boolean suggestedPrefixIsCOSMICLegacyPrefix() {
		return this.getSuggestedPrefix() != null &&
			this.getSuggestedPrefix().equalsIgnoreCase(COSMICUpdateUtil.COSMIC_LEGACY_PREFIX);
	}

	private void updateUsingCOSVIdentifier() {
		updateCOSMICDatabaseIdentifierInstance(this.getCosvIdentifier());
	}

	private void updateUsingSuggestedCOSMICPrefix() {
		SimpleInstance cosmicDatabaseIdentifierInstance = this.getCosmicDatabaseIdentifierInstance();
		String currentCOSMICIdentifier = (String)
			cosmicDatabaseIdentifierInstance.getAttribute(ReactomeJavaConstants.identifier);
		// If the current identifier already begins with "C" then leave it alone.
		// This code is for updating numeric identifiers that have a suggested prefix.
		if (!COSMICUpdateUtil.stringStartsWithC(currentCOSMICIdentifier.toUpperCase())) {
			String newCOSMICIdentifier = this.getSuggestedPrefix() + currentCOSMICIdentifier;
			updateCOSMICDatabaseIdentifierInstance(newCOSMICIdentifier);
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
	 */
	private void updateCOSMICDatabaseIdentifierInstance(String identifierValue) {
		SimpleInstance cosmicDatabaseIdentifierInstance = getCosmicDatabaseIdentifierInstance();
		cosmicDatabaseIdentifierInstance.setAttribute(ReactomeJavaConstants.identifier, identifierValue);
		cosmicDatabaseIdentifierInstance.setDisplayName(generateDisplayName(cosmicDatabaseIdentifierInstance));

		updateInDatabase(cosmicDatabaseIdentifierInstance);
	}

	private void updateInDatabase(SimpleInstance instance) {
		instance.setDefaultPersonId(getPersonId());
		curatorToolAPI.commit(instance);
	}

	private long getDbID() {
		return getCosmicDatabaseIdentifierInstance().getDbId();
	}
}
