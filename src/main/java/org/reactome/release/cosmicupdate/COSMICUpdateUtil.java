package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidAttributeException;

public class COSMICUpdateUtil
{
	static final String COSMIC_LEGACY_PREFIX = "COSM";
	private static final String COSMIC_FUSION_ID = "FUSION_ID";
	private static final String COSMIC_GENOMIC_MUTATION_ID = "GENOMIC_MUTATION_ID";
	private static final String COSMIC_MUTATION_ID = "MUTATION_ID";
	private static final String COSMIC_LEGACY_MUTATION_ID = "LEGACY_MUTATION_ID";
	static final String COSMIC_FUSION_PREFIX = "COSF";
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * Validate the identifiers in the database by comparing them to the identifiers in the file.
	 * @param updates
	 * @param COSMICFusionExportFile
	 * @param COSMICMutationTrackingFile
	 * @param COSMICMutantExportFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	static void validateIdentifiersAgainstFiles(Map<String, COSMICIdentifierUpdater> updates, String COSMICFusionExportFile, String COSMICMutationTrackingFile, String COSMICMutantExportFile) throws IOException, FileNotFoundException
	{
		Set<String> fusionIDs = updates.keySet().parallelStream().filter(id -> id.toUpperCase().startsWith(COSMIC_FUSION_PREFIX)).map(id -> id.toUpperCase().replaceAll(COSMIC_FUSION_PREFIX,"") ).collect(Collectors.toSet());
		logger.info("Now checking with CosmicFusionExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMICFusionExportFile), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')); )
		{
			parser.forEach( record -> {
				String fusionID = record.get(COSMIC_FUSION_ID);
				if (fusionIDs.contains(fusionID))
				{
					updates.get(COSMIC_FUSION_PREFIX+fusionID).setValid(true);
				}
			});
		}
		
		logger.info("Now checking with CosmicMutationTracking.tsv...");
		// Now we need to look through the HUGE file from COSMIC and see if we can map the identifiers...
		try(CSVParser parser = new CSVParser(new FileReader(COSMICMutationTrackingFile), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));)
		{
			parser.forEach(record -> {
				String legacyID = record.get(COSMIC_LEGACY_MUTATION_ID);
				String mutationID = record.get(COSMIC_MUTATION_ID);
				String genomicID = record.get(COSMIC_GENOMIC_MUTATION_ID);
				if (updates.containsKey(legacyID))
				{
					updates.get(legacyID).getMutationIDs().add(mutationID);
					updates.get(legacyID).setCosvIdentifier(genomicID);
				}
			
			});
		}
		logger.info("Now checking with CosmicMutantExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMICMutantExportFile), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')); )
		{
			parser.forEach(record -> {
				String legacyID = record.get(COSMIC_LEGACY_MUTATION_ID);
				String mutationID = record.get(COSMIC_MUTATION_ID);
				String genomicID = record.get(COSMIC_GENOMIC_MUTATION_ID);
				if (updates.containsKey(legacyID))
				{
					updates.get(legacyID).setValid(true); // only VALID if in MutantExport...
					updates.get(legacyID).getMutationIDs().add(mutationID);
					updates.get(legacyID).setCosvIdentifier(genomicID);
				}
			});
		}
	}

	/**
	 * Determines the prefixes for COSMIC identifiers. The rule is:
	 * IF an object has EWASes and there is an EWAS with a FragmentReplacedModification or a FragmentInsertionModification whose referenceSequence 
	 * is NOT the referenceEntity of the EWAS... then the suggested prefix will be COSF (for Fusion), otherwise, COSM is suggested.
	 * @param cosmicObjects
	 * @return
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws IOException
	 */
	static Map<String, COSMICIdentifierUpdater> determinePrefixes(Collection<GKInstance> cosmicObjects) throws InvalidAttributeException, Exception, IOException
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		try(CSVPrinter nonEWASPrinter = new CSVPrinter(new FileWriter("nonEWASObjectsWithCOSMICIdentifiers.csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier", "Referred-to-by CadidateSet (or other non-EWAS)"));
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
						identifiersWithNoReferrerPrinter.printRecord(identifier);
					}
					else
					{
						checkEWASes(nonEWASPrinter, identifier, updateRecord, EWASes);
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

	/**
	 * Checks EWASes to see if they have modifiedResidues that have a referenceSequence that is NOT the same as the EWASes referenceEntity.
	 * @param nonEWASPrinter
	 * @param identifier
	 * @param updateRecord
	 * @param EWASes
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws IOException
	 */
	private static void checkEWASes(CSVPrinter nonEWASPrinter, String identifier, COSMICIdentifierUpdater updateRecord, Collection<GKInstance> EWASes) throws InvalidAttributeException, Exception, IOException
	{
		String prefix;
		for (GKInstance ewas : EWASes)
		{
			if (ewas.getSchemClass().getName().equals(ReactomeJavaConstants.EntityWithAccessionedSequence))
			{
				GKInstance refSequence = (GKInstance) ewas.getAttributeValue(ReactomeJavaConstants.referenceEntity);
				// get hasModifiedResidue
				@SuppressWarnings("unchecked")
				List<GKInstance> modResidues = (List<GKInstance>) ewas.getAttributeValuesList(ReactomeJavaConstants.hasModifiedResidue);
				
				boolean foundMismatchedRefSequence = checkReferenceSequences(refSequence, modResidues);
				
				// If we get to the end of the loop and there is a mismatch, then COSF.
				prefix = foundMismatchedRefSequence ? COSMIC_FUSION_PREFIX : COSMIC_LEGACY_PREFIX;
				updateRecord.setSuggestedPrefix(prefix);
			}
			else
			{
				nonEWASPrinter.printRecord(identifier, ewas.toString());
			}
		}
	}

	/**
	 * Checks modifiedResidues (only FragmentReplacedModification and FragmentInsertionModification are of interest) to see if they match
	 * refSequence. 
	 * @param refSequence
	 * @param modResidues
	 * @return
	 * @throws InvalidAttributeException
	 * @throws Exception
	 */
	private static boolean checkReferenceSequences(GKInstance refSequence, List<GKInstance> modResidues)
			throws InvalidAttributeException, Exception {
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
		return foundMismatchedRefSequence;
	}

	/**
	 * Gets COSMIC identifiers from the database.
	 * @param adaptor
	 * @return
	 * @throws SQLException
	 * @throws Exception
	 * @throws InvalidAttributeException
	 */
	static Collection<GKInstance> getCOSMICIdentifiers(MySQLAdaptor adaptor) throws SQLException, Exception, InvalidAttributeException
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
			logger.error("Too many COSMIC refDBs: {}", refDBs.size());
			logger.error("Cannot proceeed!");
			System.exit(1);
		}
		
		@SuppressWarnings("unchecked")
		Collection<GKInstance> cosmicObjects = adaptor.fetchInstanceByAttribute(ReactomeJavaConstants.DatabaseIdentifier, ReactomeJavaConstants.referenceDatabase, " = ", cosmicRefDB.getAttributeValue(ReactomeJavaConstants.DB_ID));
		return cosmicObjects;
	}
}
