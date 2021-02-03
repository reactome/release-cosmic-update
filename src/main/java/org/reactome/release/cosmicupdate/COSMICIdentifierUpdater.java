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
 * Implements <code>Comparable</code> to make sorting easier for reporting purposes. Does *not* override the <code>equals</code> method.
 * @author sshorser
 *
 */
public class COSMICIdentifierUpdater implements Comparable<COSMICIdentifierUpdater>
{
	private static final Logger logger = LogManager.getLogger();
	private static GKInstance instanceEditNewCOSV;
	private static GKInstance instanceEditPrependCOSM;
	private String identifier;
	private long dbID;
	private String suggestedPrefix;
	private boolean valid;
	private Set<String> mutationIDs = new HashSet<>();
	private String cosvIdentifier;

	public String getIdentifier()
	{
		return identifier;
	}
	public void setIdentifier(String identifier)
	{
		this.identifier = identifier;
	}
	public long getDbID()
	{
		return dbID;
	}
	public void setDbID(long dbID)
	{
		this.dbID = dbID;
	}
	public String getSuggestedPrefix()
	{
		return suggestedPrefix;
	}
	public void setSuggestedPrefix(String suggestedPrefix)
	{
		this.suggestedPrefix = suggestedPrefix;
	}
	public boolean isValid()
	{
		return valid;
	}
	public void setValid(boolean valid)
	{
		this.valid = valid;
	}
	public Set<String> getMutationIDs()
	{
		return mutationIDs;
	}
	public void setMutationIDs(Set<String> mutationIDs)
	{
		this.mutationIDs = mutationIDs;
	}
	public String getCosvIdentifier()
	{
		return cosvIdentifier;
	}
	public void setCosvIdentifier(String cosvIdentifier)
	{
		this.cosvIdentifier = cosvIdentifier;
	}
	
	@Override
	public String toString()
	{
		return "[" + this.getDbID() + "; " 
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
	public int compareTo(COSMICIdentifierUpdater other)
	{
		if (other.valid && !this.valid)
		{
			return -1;
		}
		if (!other.valid && this.valid)
		{
			return 1;
		}
		if (other.valid == this.valid)
		{
			return this.identifier.compareTo(other.identifier);
		}
		return 0;
	}

	/**
	 * Perform an update of a COSMIC identifier.
	 * @param adaptor - the database adapter to use.
	 * @param creatorID - the DB_ID of the Creator of this update.
	 * @throws Exception
	 */
	public void updateIdentfier(MySQLAdaptor adaptor, long creatorID) throws Exception
	{
		// If there is a COSV identifier, we'll update using that.
		if (this.getCosvIdentifier() != null && !this.getCosvIdentifier().isEmpty())
		{
			// Create the InstanceEdit, if necessary.
			synchronized (COSMICIdentifierUpdater.class)
			{
				if (COSMICIdentifierUpdater.instanceEditNewCOSV == null)
				{
					COSMICIdentifierUpdater.instanceEditNewCOSV = InstanceEditUtils.createDefaultIE(adaptor, creatorID, true, "Identifier was automatically updated to new \"COSV\" identifier by COSMIC Update process.");
				}
			}
			GKInstance identifierObject = adaptor.fetchInstance( this.getDbID());
			
			updateIdentifierObject(adaptor, COSMICIdentifierUpdater.instanceEditNewCOSV, identifierObject, this.getCosvIdentifier());
		}
		// If no COSV identifier was found, update using the suggested prefix (determined computationally).
		else if (this.getSuggestedPrefix() != null && this.getSuggestedPrefix().equalsIgnoreCase(COSMICUpdateUtil.COSMIC_LEGACY_PREFIX))
		{
			GKInstance identifierObject = adaptor.fetchInstance( this.getDbID());
			String currentIdentifier = (String) identifierObject.getAttributeValue(ReactomeJavaConstants.identifier);
			// If the current identifier already begins with "C" then leave it alone.
			// This code is for updating numeric identifiers that have a suggested prefix.
			if (!COSMICUpdateUtil.stringStartsWithC(currentIdentifier.toUpperCase()))
			{
				// Create the InstanceEdit, if necessary.
				synchronized (COSMICIdentifierUpdater.class)
				{
					if (COSMICIdentifierUpdater.instanceEditPrependCOSM == null)
					{
						COSMICIdentifierUpdater.instanceEditPrependCOSM = InstanceEditUtils.createDefaultIE(adaptor, creatorID, true, "Identifier was automatically prepended with \"COSM\" by COSMIC Update process.");
					}
				}
				updateIdentifierObject(adaptor, COSMICIdentifierUpdater.instanceEditPrependCOSM, identifierObject, this.getSuggestedPrefix() + currentIdentifier);
			}
		}
		// Some identifiers won't have a COSV identifier in the COSMIC files, and they might not have a suggested prefix either.
		else
		{
			logger.info("No suggested prefix OR corresponding COSV identifier for {} (DBID: {}) - identifier will not be updated.", this.getIdentifier(), this.getDbID());
		}
	}
	
	/**
	 * Executes an update on an instance.
	 * Sets the identifier attribute of <code>identifierObject</code> to the value of <code>identifierValue</code>.
	 * <code>identifierObject</code> (which must be an InstanceEdit) will also have <code>modifiedForCOSMICUpdate</code> added to its <code>modified</code> list.
	 * The display name of <code>identifierObject</code> will also be regenerated to reflect changes in <code>identifierValue</code>.
	 * @param adaptor
	 * @param modifiedForCOSMICUpdate An InstanceEdit which explains why an instance was modified.
	 * @param identifierObject An object that represents a COSMIC identifier.
	 * @param identifierValue An identifier value that will be set on <code>identifierObject</code>
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws InvalidAttributeValueException
	 */
	private void updateIdentifierObject(MySQLAdaptor adaptor, GKInstance modifiedForCOSMICUpdate, GKInstance identifierObject, String identifierValue) throws InvalidAttributeException, Exception, InvalidAttributeValueException
	{
		List<GKInstance> modifications = (List<GKInstance>) identifierObject.getAttributeValuesList(ReactomeJavaConstants.modified);
		String newDisplayName = InstanceDisplayNameGenerator.generateDisplayName(identifierObject);
		modifications.add(modifiedForCOSMICUpdate);
		
		identifierObject.setAttributeValue(ReactomeJavaConstants.modified, modifications);
		identifierObject.setAttributeValue(ReactomeJavaConstants.identifier, identifierValue);
		identifierObject.setAttributeValue(ReactomeJavaConstants._displayName, newDisplayName);
		
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.identifier);
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.modified);
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants._displayName);
	}
}
