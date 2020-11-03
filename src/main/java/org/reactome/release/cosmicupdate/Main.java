package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.InstanceDisplayNameGenerator;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidAttributeException;
import org.gk.schema.InvalidAttributeValueException;
import org.reactome.release.common.ReleaseStep;
import org.reactome.release.common.database.InstanceEditUtils;



public class Main extends ReleaseStep
{
	private static final String COSMIC_MUTANT_EXPORT = "./CosmicMutantExport.tsv";
	private static final String COSMIC_FUSION_EXPORT = "./CosmicFusionExport.tsv";
	private static final String COSMIC_MUTATION_TRACKING = "./CosmicMutationTracking.tsv";
	/*
	// Need to track: Old identifiers, suggested prefix (if applicable), validity, DB ID of Identifier object (and referrer(s) ?)
	private class COSMICIdentifierUpdateRecord implements Comparable<COSMICIdentifierUpdateRecord>
	{
		// This is so ugly... I wish we could move to a newer version of Java that has Records!
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
		@Override
		public int compareTo(COSMICIdentifierUpdateRecord arg0)
		{
			if (arg0.valid && !this.valid)
			{
				return -1;
			}
			if (!arg0.valid && this.valid)
			{
				return 1;
			}
			if (arg0.valid == this.valid)
			{
				return this.identifier.compareTo(arg0.identifier);
			}
			return 0;
		}
	}
	*/
	private static final Logger logger = LogManager.getLogger();
	private static long creatorID;
	public static void main(String... args)
	{
		try
		{
			MySQLAdaptor adaptor = new MySQLAdaptor("localhost", "gk_central", "root", "root");
			Collection<GKInstance> cosmicObjects = getCOSMICIdentifiers(adaptor);
			logger.info("{} COSMIC identifiers", cosmicObjects.size());
			// Filter the identifiers to exclude the COSV prefixes. 
			List<GKInstance> filteredCosmicObjects = cosmicObjects.parallelStream().filter(inst -> {
				try
				{
					return !((String)inst.getAttributeValue(ReactomeJavaConstants.identifier)).toUpperCase().startsWith("COSV");
				}
				catch (InvalidAttributeException e)
				{
					e.printStackTrace();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				return false;
			}).collect(Collectors.toList());
			logger.info("{} filtered COSMIC identifiers", filteredCosmicObjects.size());
			// Build a structure that lets us lookup database objects by their COSMIC identifier.
//			Map<String, Long> oldIdentifierToDBIDMap = new HashMap<String, Long>();
//			Set<String> suggestedIdentifiers = new HashSet<String>();
//			Set<String[]> identifiersWithNewPrefixes = new HashSet<>();
			Main cosmicUpdater = new Main();
			Map<String, COSMICIdentifierUpdater> updates = cosmicUpdater.determinePrefixes(filteredCosmicObjects/* , oldIdentifierToDBIDMap, suggestedIdentifiers, identifiersWithNewPrefixes */);
			
			validateIdentifiersAgainstFiles(updates);
			printIdentifierUpdateReport(updates);
			updateIdentifiers(adaptor, updates);
		}
		catch (SQLException e)
		{
			logger.error("Error with SQL Connection", e);
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception caught", e);
		}
		logger.info("All done.");
		
	}

//	private static void updateIdentifiers(MySQLAdaptor adaptor, Map<String, COSMICIdentifierUpdateRecord> updates) throws Exception, InvalidAttributeException, InvalidAttributeValueException
//	{
//		GKInstance updateWithNewCOSV = InstanceEditUtils.createDefaultIE(adaptor, 8939149L, true, "Identifier was automatically updated to new \"COSV\" identifier by COSMIC Update process.");
//		GKInstance updatePrependCOSM = InstanceEditUtils.createDefaultIE(adaptor, 8939149L, true, "Identifier was automatically prepended with \"COSM\" by COSMIC Update process.");
//		int cosmPrependCount = 0;
//		int cosvUpdateCount = 0;
//		// At this point, we should be able to replace COSM identifiers with COSV identifiers - if there _is_ one. If not, do nothing!
//		logger.info("Now updating identifiers...");
//		for (COSMICIdentifierUpdateRecord record : updates.values())
//		{
//			if (record.getCosvIdentifier() != null && !record.getCosvIdentifier().isEmpty())
//			{
//				GKInstance identifierObject = adaptor.fetchInstance( record.getDbID());
//				identifierObject.setAttributeValue(ReactomeJavaConstants.identifier, record.getCosvIdentifier());
//				
//				updateIdentifierObject(adaptor, updateWithNewCOSV, identifierObject);
//				cosvUpdateCount++;
//			}
//			else if (record.getSuggestedPrefix() != null && record.getSuggestedPrefix().toUpperCase().equals("COSM"))
//			{
//				GKInstance identifierObject = adaptor.fetchInstance( record.getDbID());
//				String currentIdentifier = (String) identifierObject.getAttributeValue(ReactomeJavaConstants.identifier);
//				if (!currentIdentifier.toUpperCase().startsWith("C"))
//				{
//					identifierObject.setAttributeValue(ReactomeJavaConstants.identifier, record.getSuggestedPrefix() + currentIdentifier);
//					updateIdentifierObject(adaptor, updatePrependCOSM, identifierObject);
//					cosmPrependCount++;
//				}
//			}
//			else
//			{
//				logger.info("No suggested prefix OR corresponding COSV identifier for {} (DBID: {}) - identifier will not be updated.", record.getIdentifier(), record.getDbID());
//			}
//		}
//		// cleanup - delete the instance edits if unused.
//		if (cosmPrependCount == 0)
//		{
//			logger.info("No identifiers were prepended with \"COSM\"");
//			adaptor.deleteByDBID(updatePrependCOSM.getDBID());
//		}
//		if (cosvUpdateCount == 0)
//		{
//			logger.info("No identifiers were updated to \"COSV\" identifiers");
//			adaptor.deleteByDBID(updateWithNewCOSV.getDBID());
//		}
//	}

	private static void updateIdentifiers(MySQLAdaptor adaptor, Map<String, COSMICIdentifierUpdater> updates)
	{
		for (COSMICIdentifierUpdater updator : updates.values())
		{
			try
			{
				updator.updateIdentfier(adaptor, creatorID);
			}
			catch (Exception e)
			{
				logger.error("Exception caught while trying to update identifier: "+updator.toString()+" ; Exception is: ",e);
			}
		}		
	}

	//	private static void updateIdentifierObject(MySQLAdaptor adaptor, GKInstance modifiedForCOSMICUpdate,
//			GKInstance identifierObject) throws InvalidAttributeException, Exception, InvalidAttributeValueException {
//		List<GKInstance> modifications = identifierObject.getAttributeValuesList(ReactomeJavaConstants.modified);
//
//		modifications.add(modifiedForCOSMICUpdate);
//		identifierObject.setAttributeValue(ReactomeJavaConstants.modified, modifications);
//		
//		String newDisplayName = InstanceDisplayNameGenerator.generateDisplayName(identifierObject);
//		identifierObject.setAttributeValue(ReactomeJavaConstants._displayName, newDisplayName);
//		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.identifier);
//		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants.modified);
//		adaptor.updateInstanceAttribute(identifierObject, ReactomeJavaConstants._displayName);
//	}
//
	private static void printIdentifierUpdateReport(Map<String, COSMICIdentifierUpdater> updates) throws IOException
	{
		try(CSVPrinter printer = new CSVPrinter(new FileWriter("./valid-identifiers.csv"), CSVFormat.DEFAULT.withHeader("DB_ID", "Identifier", "Suggested Prefix", "Valid?", "COSV identifier", "Mutation IDs")))
		{
			for (COSMICIdentifierUpdater record : updates.values().parallelStream().sorted().collect(Collectors.toList()))
			{
				printer.printRecord(record.getDbID(), record.getIdentifier(), record.getSuggestedPrefix(), record.isValid(), record.getCosvIdentifier(), record.getMutationIDs().toString());
			}
		}
	}

	private static void validateIdentifiersAgainstFiles(Map<String, COSMICIdentifierUpdater> updates) throws IOException, FileNotFoundException
	{
		Set<String> fusionIDs = updates.keySet().parallelStream().filter(id -> id.toUpperCase().startsWith("COSF")).map(id -> id.toUpperCase().replaceAll("COSF","") ).collect(Collectors.toSet());
		logger.info("Now checking with CosmicFusionExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMIC_FUSION_EXPORT), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));
		/*
		 * CSVPrinter printer = new CSVPrinter(new FileWriter("./cosf-mappings.csv"),
		 * CSVFormat.DEFAULT.withHeader("DB_ID", "Legacy ID", "Mutation ID",
		 * "Genomic ID") )
		 */)
		{
			parser.forEach( record -> {
				String fusionID = record.get("FUSION_ID");
				if (fusionIDs.contains(fusionID))
				{
					updates.get("COSF"+fusionID).setValid(true);
				}
			});
		}
		
		logger.info("Now checking with CosmicMutationTracking.tsv...");
		// Now we need to look through the HUGE file from COSMIC and see if we can map the identifiers...
		try(CSVParser parser = new CSVParser(new FileReader(COSMIC_MUTATION_TRACKING), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));
			/*CSVPrinter printer = new CSVPrinter(new FileWriter("./mappings.csv"), CSVFormat.DEFAULT.withHeader("DB_ID", "Legacy ID", "Mutation ID", "Genomic ID") ) */)
		{
			parser.forEach(record -> {
				String legacyID = record.get("LEGACY_MUTATION_ID");
				String mutationID = record.get("MUTATION_ID");
				String genomicID = record.get("GENOMIC_MUTATION_ID");
				if (updates.containsKey(legacyID))
				{
//						logger.info("Mutation ID: {} in Reactome has Legacty ID: {} and Genomic Mutation ID: {}", mutationID, legacyID, genomicID);
//					try
//					{
//						updates.get(legacyID).setValid(true);
//						printer.printRecord(updates.get(legacyID).toString(), legacyID, mutationID, genomicID);
						updates.get(legacyID).getMutationIDs().add(mutationID);
						updates.get(legacyID).setCosvIdentifier(genomicID);
//					}
//					catch (IOException e)
//					{
//						e.printStackTrace();
//					}
				}
			
			});
		}
		logger.info("Now checking with CosmicMutantExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMIC_MUTANT_EXPORT), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));
			CSVPrinter printer = new CSVPrinter(new FileWriter("./mappings.csv", true), CSVFormat.DEFAULT.withHeader("DB_ID", "Legacy ID", "Mutation ID", "Genomic ID") ))
		{
			parser.forEach(record -> {
				String legacyID = record.get("LEGACY_MUTATION_ID");
				String mutationID = record.get("MUTATION_ID");
				String genomicID = record.get("GENOMIC_MUTATION_ID");
				if (updates.containsKey(legacyID))
				{
						updates.get(legacyID).setValid(true); // only VALID if in MutantExport...
						updates.get(legacyID).getMutationIDs().add(mutationID);
						updates.get(legacyID).setCosvIdentifier(genomicID);
				}
			});
		}
	}

	private Map<String, COSMICIdentifierUpdater> determinePrefixes(Collection<GKInstance> cosmicObjects/*,
			Map<String, Long> oldIdentifierToDBIDMap, Set<String> suggestedIdentifiers,
			Set<String[]> identifiersWithNewPrefixes*/)
		throws InvalidAttributeException, Exception, IOException
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		try(//CSVPrinter printer = new CSVPrinter(new FileWriter("COSMICIdentifierUpdateReport.csv"), CSVFormat.DEFAULT.withHeader("Old COSMIC identifier", "Referred-to-by EWAS", "Suggested Prefix"));
			CSVPrinter setPrinter = new CSVPrinter(new FileWriter("CandidateSetsWithCOSMICIdentifiers.csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier", "Referred-to-by CadidateSet (or other non-EWAS)"));
			CSVPrinter identifiersWithNoReferrerPrinter = new CSVPrinter(new FileWriter("COSMICIdentifiersNoReferrers.csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier")))
		{
		
			for (GKInstance inst : cosmicObjects)
			{
				String identifier = (String)inst.getAttributeValue(ReactomeJavaConstants.identifier);
			
				COSMICIdentifierUpdater updateRecord = new COSMICIdentifierUpdater();
				updateRecord.setIdentifier(identifier);
				updateRecord.setDbID(inst.getDBID());
				//if (!identifier.startsWith("C"))
				// Also, try to determine prefix. The rule is, if the EWAS has
				// a fragmentModification whose referenceSequence is different
				// than the EWAS’s referenceSequence  (COSF in this case and COSM otherwise).
				{
					String prefix = "";
					
					@SuppressWarnings("unchecked")
					Collection<GKInstance> EWASes = inst.getReferers(ReactomeJavaConstants.crossReference);
					if (EWASes == null || EWASes.isEmpty())
					{
//							logger.info("{} has no referrers", inst.toString());
						identifiersWithNoReferrerPrinter.printRecord(identifier);
					}
					else
					{
						for (GKInstance ewas : EWASes)
						{
							if (ewas.getSchemClass().getName().equals(ReactomeJavaConstants.EntityWithAccessionedSequence))
							{
								GKInstance refSequence = (GKInstance) ewas.getAttributeValue(ReactomeJavaConstants.referenceEntity);
								// get hasModifiedResidue
								@SuppressWarnings("unchecked")
								List<GKInstance> modResidues = (List<GKInstance>) ewas.getAttributeValuesList(ReactomeJavaConstants.hasModifiedResidue);
								
								boolean foundMismatchedRefSequence = false;
								int i = 0;
								while (!foundMismatchedRefSequence && i < modResidues.size())
								{
									GKInstance modResidue = modResidues.get(i);
									if (modResidue.getSchemClass().getName().contains(ReactomeJavaConstants.FragmentReplacedModification)
										|| modResidue.getSchemClass().getName().contains(ReactomeJavaConstants.FragmentInsertionModification))
									{
										GKInstance residueRefSequence = (GKInstance) modResidue.getAttributeValue(ReactomeJavaConstants.referenceSequence);
										foundMismatchedRefSequence = !residueRefSequence.getDBID().equals(refSequence.getDBID());
									}
									i++;
								}
								
								// If we get to the end of the loop and there is a mismatch, then COSF.
								prefix = foundMismatchedRefSequence ? "COSF" : "COSM" ;
								updateRecord.setSuggestedPrefix(prefix);
//									logger.info("EWAS: {}; old COSMIC identifier: {}; prefix: {}", ewas.toString(), inst.getAttributeValue(ReactomeJavaConstants.identifier), prefix);
//								if (!identifier.startsWith(prefix))
//								{
//									oldIdentifierToDBIDMap.put(identifier, inst.getDBID() );
//									identifiersWithNewPrefixes.add(new String[]{identifier, ewas.toString(), prefix});
//									suggestedIdentifiers.add(prefix + identifier);
//								}
							}
							else
							{
								setPrinter.printRecord(identifier, ewas.toString());
							}
						}
					}
				}
				if (identifier.startsWith("C"))
				{
					// Consider: is it possible that we might get 1:n mapping for some identifier?
					updates.put(updateRecord.getIdentifier(), updateRecord);
				}
				else
				{
					updates.put(updateRecord.getSuggestedPrefix() + updateRecord.getIdentifier(), updateRecord);
				}
			}
		}
		return updates;
	}

	private static Collection<GKInstance> getCOSMICIdentifiers(MySQLAdaptor adaptor) throws SQLException, Exception, InvalidAttributeException
	{
		
		@SuppressWarnings("unchecked")
		Collection<GKInstance> refDBs = adaptor.fetchInstanceByAttribute(ReactomeJavaConstants.ReferenceDatabase, ReactomeJavaConstants.name, " = ", "COSMIC");
		GKInstance cosmicRefDB = null;
		if (refDBs.size() == 1)
		{
			cosmicRefDB = refDBs.stream().findFirst().get();
		}
		else
		{
			logger.info("Too many COSMIC refDBs: {}", refDBs.size());
			logger.info("Cannot proceeed!");
			System.exit(1);
		}
		
		@SuppressWarnings("unchecked")
		Collection<GKInstance> cosmicObjects = adaptor.fetchInstanceByAttribute(ReactomeJavaConstants.DatabaseIdentifier, ReactomeJavaConstants.referenceDatabase, " = ", cosmicRefDB.getAttributeValue(ReactomeJavaConstants.DB_ID));
		return cosmicObjects;
	}

	@Override
	public void executeStep(Properties props) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
