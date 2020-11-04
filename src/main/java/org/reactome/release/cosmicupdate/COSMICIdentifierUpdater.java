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
	private static GKInstance updateWithNewCOSV;
	private static GKInstance updatePrependCOSM;
	private String identifier;
	private Long dbID;
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
	public Long getDbID()
	{
		return dbID;
	}
	public void setDbID(Long dbID)
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
					+ this.getMutationIDs().stream().reduce("", (s, t) -> s + t + ",") + ")" + "]";
	}
	
	/**
	 * Implementation of compareTo ensures that these are sorted by validity, and then by identifier. 
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

	public void updateIdentfier(MySQLAdaptor adaptor, long creatorID) throws Exception
	{
		if (this.getCosvIdentifier() != null && !this.getCosvIdentifier().isEmpty())
		{
			// Create the InstanceEdit, if necessary.
			synchronized (COSMICIdentifierUpdater.updateWithNewCOSV)
			{
				if (COSMICIdentifierUpdater.updateWithNewCOSV == null)
				{
					COSMICIdentifierUpdater.updateWithNewCOSV = InstanceEditUtils.createDefaultIE(adaptor, creatorID, true, "Identifier was automatically updated to new \"COSV\" identifier by COSMIC Update process.");
				}
			}
			GKInstance identifierObject = adaptor.fetchInstance( this.getDbID());
			identifierObject.setAttributeValue(ReactomeJavaConstants.identifier, this.getCosvIdentifier());
			
			updateIdentifierObject(adaptor, COSMICIdentifierUpdater.updateWithNewCOSV, identifierObject);
		}
		else if (this.getSuggestedPrefix() != null && this.getSuggestedPrefix().equalsIgnoreCase(COSMICUpdateUtil.COSMIC_LEGACY_PREFIX))
		{
			GKInstance identifierObject = adaptor.fetchInstance( this.getDbID());
			String currentIdentifier = (String) identifierObject.getAttributeValue(ReactomeJavaConstants.identifier);
			if (!currentIdentifier.toUpperCase().startsWith("C"))
			{
				identifierObject.setAttributeValue(ReactomeJavaConstants.identifier, this.getSuggestedPrefix() + currentIdentifier);
				// Create the InstanceEdit, if necessary.
				synchronized (COSMICIdentifierUpdater.updatePrependCOSM)
				{
					if (COSMICIdentifierUpdater.updatePrependCOSM == null)
					{
						COSMICIdentifierUpdater.updatePrependCOSM = InstanceEditUtils.createDefaultIE(adaptor, creatorID, true, "Identifier was automatically prepended with \"COSM\" by COSMIC Update process.");
					}
				}
				updateIdentifierObject(adaptor, COSMICIdentifierUpdater.updatePrependCOSM, identifierObject);
			}
		}
		else
		{
			logger.info("No suggested prefix OR corresponding COSV identifier for {} (DBID: {}) - identifier will not be updated.", this.getIdentifier(), this.getDbID());
		}
	}
	

	
	private void updateIdentifierObject(MySQLAdaptor adaptor, GKInstance modifiedForCOSMICUpdate, GKInstance identifierObject) throws InvalidAttributeException, Exception, InvalidAttributeValueException
	{
		List<GKInstance> modifications = (List<GKInstance>) identifierObject.getAttributeValuesList(ReactomeJavaConstants.modified);

		modifications.add(modifiedForCOSMICUpdate);
		identifierObject.setAttributeValue(ReactomeJavaConstants.modified, modifications);
		
		String newDisplayName = InstanceDisplayNameGenerator.generateDisplayName(identifierObject);
		identifierObject.setAttributeValue(ReactomeJavaConstants._displayName, newDisplayName);
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.identifier);
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.modified);
		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants._displayName);
	}
}
