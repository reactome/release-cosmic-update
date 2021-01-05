package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

/*
 * Thie class contains utility methods that are to be used for updating COSMIC identifiers.
 */
public class COSMICUpdateUtil
{
	static final String COSMIC_LEGACY_PREFIX = "COSM";
	private static final String COSMIC_FUSION_ID = "FUSION_ID";
	private static final String COSMIC_GENOMIC_MUTATION_ID = "GENOMIC_MUTATION_ID";
	private static final String COSMIC_MUTATION_ID = "MUTATION_ID";
	private static final String COSMIC_LEGACY_MUTATION_ID = "LEGACY_MUTATION_ID";
	static final String COSMIC_FUSION_PREFIX = "COSF";
	private static final Logger logger = LogManager.getLogger();
	private static String dateSuffix;
	private static String reportsDirectoryPath = "reports";
	// Private constructor to prevent instantiation of utility class
	private COSMICUpdateUtil()
	{
		// ...no-op
	}
	
	static
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_kkmmss");
		COSMICUpdateUtil.dateSuffix = formatter.format(LocalDateTime.now());
	}
	
	/**
	 * Validate the identifiers in the database by comparing them to the identifiers in the file.
	 * @param updaters A map of updaters, keyed by COSMIC identifier.
	 * @param COSMICFusionExportFile The path to the COSMIC Fusion Export file.
	 * @param COSMICMutationTrackingFile The path o the COSMIC Mutation Tracking file.
	 * @param COSMICMutantExportFile The path to the COMSIC Mutant Export file.
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	static void validateIdentifiersAgainstFiles(Map<String, COSMICIdentifierUpdater> updaters, String COSMICFusionExportFile, String COSMICMutationTrackingFile, String COSMICMutantExportFile) throws IOException, FileNotFoundException
	{
		Set<String> fusionIDs = updaters.keySet().parallelStream().filter(id -> id.toUpperCase().startsWith(COSMIC_FUSION_PREFIX)).map(id -> id.toUpperCase().replaceAll(COSMIC_FUSION_PREFIX,"") ).collect(Collectors.toSet());
		logger.info("Now checking with CosmicFusionExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMICFusionExportFile), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')); )
		{
			parser.forEach( record -> {
				String fusionID = record.get(COSMIC_FUSION_ID);
				if (fusionIDs.contains(fusionID))
				{
					updaters.get(COSMIC_FUSION_PREFIX+fusionID).setValid(true);
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
				if (updaters.containsKey(legacyID))
				{
					updaters.get(legacyID).getMutationIDs().add(mutationID);
					updaters.get(legacyID).setCosvIdentifier(genomicID);
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
				if (updaters.containsKey(legacyID))
				{
					updaters.get(legacyID).setValid(true); // only VALID if in MutantExport...
					updaters.get(legacyID).getMutationIDs().add(mutationID);
					updaters.get(legacyID).setCosvIdentifier(genomicID);
				}
			});
		}
	}

	/**
	 * Determines the prefixes for COSMIC identifiers. The rule is:
	 * IF an object has EWASes and there is an EWAS with a FragmentReplacedModification or a FragmentInsertionModification whose referenceSequence 
	 * is NOT the referenceEntity of the EWAS... then the suggested prefix will be COSF (for Fusion), otherwise, COSM is suggested.
	 * @param cosmicObjects Objects that are identified by a COSMIC identifier.
	 * @return A map of <code>COSMICIdentifierUpdater</code>, keyed by COSMIC identifier.
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws IOException
	 */
	static Map<String, COSMICIdentifierUpdater> determinePrefixes(Collection<GKInstance> cosmicObjects) throws InvalidAttributeException, Exception, IOException
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		
		// Create the reports directory if it's missing.
		if (!Files.exists(Paths.get(COSMICUpdateUtil.reportsDirectoryPath)))
		{
			Files.createDirectories(Paths.get(COSMICUpdateUtil.reportsDirectoryPath));
		}
		
		try(CSVPrinter nonEWASPrinter = new CSVPrinter(new FileWriter(COSMICUpdateUtil.reportsDirectoryPath + "/nonEWASObjectsWithCOSMICIdentifiers_"+dateSuffix+".csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier", "Referred-to-by CadidateSet (or other non-EWAS)"));
			CSVPrinter identifiersWithNoReferrerPrinter = new CSVPrinter(new FileWriter(COSMICUpdateUtil.reportsDirectoryPath + "/COSMICIdentifiersNoReferrers_"+dateSuffix+".csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier")))
		{
			for (GKInstance inst : cosmicObjects)
			{
				String identifier = (String)inst.getAttributeValue(ReactomeJavaConstants.identifier);
			
				COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
				updater.setIdentifier(identifier);
				updater.setDbID(inst.getDBID());

				@SuppressWarnings("unchecked")
				Collection<GKInstance> EWASes = inst.getReferers(ReactomeJavaConstants.crossReference);
				// If NO EWASes exist, then log this information.
				if (EWASes == null || EWASes.isEmpty())
				{
					identifiersWithNoReferrerPrinter.printRecord(identifier);
				}
				else
				{
					// Check the EWASes for mismatches between referenceSequence identifier and main identifier.
					// If there's a mismatch then suggest COSF, else suggest COSM.
					checkEWASes(nonEWASPrinter, identifier, updater, EWASes);
				}
				
				// If the identifier starts with C it's not a numeric identifier.
				if (identifier.startsWith("C"))
				{
					// Consider: is it possible that we might get 1:n mapping for some identifier?
					updates.put(updater.getIdentifier(), updater);
				}
				else
				{
					updates.put(updater.getSuggestedPrefix() + updater.getIdentifier(), updater);
				}
			}
		}
		return updates;
	}

	/**
	 * Checks EWASes to see if they have modifiedResidues that have a referenceSequence that is NOT the same as the EWASes referenceEntity.
	 * The suggested prefix will be set to COSF on the update record if mismatches are found, otherwise COSM will be set.
	 * @param nonEWASPrinter CSVPrinter for reporting.
	 * @param identifier Identifier of the object being checked, used for reporting.
	 * @param updater A COSMICIdentifierUpdater whose suggested prefix will be updated.
	 * @param EWASes The EWASes to check.
	 * @throws InvalidAttributeException
	 * @throws Exception
	 * @throws IOException
	 */
	private static void checkEWASes(CSVPrinter nonEWASPrinter, String identifier, COSMICIdentifierUpdater updater, Collection<GKInstance> EWASes) throws InvalidAttributeException, Exception, IOException
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
				
				boolean foundMismatchedRefSequence = referenceSequenceMismatchesResidues(refSequence, modResidues);
				
				// If we get to the end of the loop and there is a mismatch, then COSF.
				prefix = foundMismatchedRefSequence ? COSMIC_FUSION_PREFIX : COSMIC_LEGACY_PREFIX;
				updater.setSuggestedPrefix(prefix);
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
	 * @param refSequence A Reference Sequence
	 * @param modResidues The modified residues.
	 * @return TRUE if there is a mismatch: a mismatch is when the reference sequence DBID != the modified residues's reference sequence's DBID. FALSE, otherwise.
	 * @throws InvalidAttributeException
	 * @throws Exception
	 */
	private static boolean referenceSequenceMismatchesResidues(GKInstance refSequence, List<GKInstance> modResidues) throws InvalidAttributeException, Exception
	{
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
	 * Queries the database for a ReferenceDatabase named "COSMIC" and then gets all DatabaseIdentifier objects
	 * that refer to the COSMIC ReferenceDatabase via the referenceDatabase attribute.
	 * This method will terminate the execution of the program if more than 1 "COSMIC" ReferenceDatabase is found.
	 * If you plan to add more "COSMIC" ReferenceDatabase objects, this code will need to be changed to use the <em>correct</em> "COSMIC"
	 * ReferenceDatabase.
	 * @param adaptor
	 * @return A Collection of DatabaseIdentifier objects.
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
			logger.error("Wrong number of COSMIC refDBs: {} ; only 1 was expected.", refDBs.size());
			logger.error("Cannot proceeed!");
			System.exit(1);
		}
		
		@SuppressWarnings("unchecked")
		Collection<GKInstance> cosmicObjects = adaptor.fetchInstanceByAttribute(ReactomeJavaConstants.DatabaseIdentifier, ReactomeJavaConstants.referenceDatabase, " = ", cosmicRefDB.getAttributeValue(ReactomeJavaConstants.DB_ID));
		return cosmicObjects;
	}
	
	
	/**
	 * Produces a report on identifiers. Report indicates old/"legacy" identifiers, suggested prefixes, new identifiers suggested from COSMIC files,
	 * and validity of old identifiers.
	 * @param updaters The map of identifier updaters.
	 * @throws IOException
	 */
	public static void printIdentifierUpdateReport(Map<String, COSMICIdentifierUpdater> updaters) throws IOException
	{
		// Create the reports directory if it's missing.
		if (!Files.exists(Paths.get(COSMICUpdateUtil.reportsDirectoryPath)))
		{
			Files.createDirectories(Paths.get(COSMICUpdateUtil.reportsDirectoryPath));
		}
		try(CSVPrinter printer = new CSVPrinter(new FileWriter(COSMICUpdateUtil.reportsDirectoryPath + "/COSMIC-identifiers-report_"+dateSuffix+".csv"), CSVFormat.DEFAULT.withHeader("DB_ID", "Identifier", "Suggested Prefix", "Valid (according to COSMIC files)?", "COSV identifier", "Mutation IDs", "COSMIC Search URL")))
		{
			for (COSMICIdentifierUpdater record : updaters.values().parallelStream().sorted().collect(Collectors.toList()))
			{
				// Include a COSMIC Search URL for the identifier in the report, to make it easier for Curators to follow up on identifiers that might need attention.
				String url;
				String identifierForUrl = "";
				if (record.getIdentifier().startsWith("C"))
				{
					identifierForUrl = record.getIdentifier();
				}
				else
				{
					if (record.getSuggestedPrefix() != null)
					{
						identifierForUrl = record.getSuggestedPrefix() + record.getIdentifier();
					}
					else
					{
						identifierForUrl = record.getIdentifier();
					}
				}
				url = "https://cancer.sanger.ac.uk/cosmic/search?q=" + identifierForUrl;
				printer.printRecord(record.getDbID(), record.getIdentifier(), record.getSuggestedPrefix(), record.isValid(), record.getCosvIdentifier(), record.getMutationIDs().toString(), url);
			}
		}
	}

	public synchronized static String getReportsDirectoryPath()
	{
		return reportsDirectoryPath;
	}

	public synchronized static void setReportsDirectoryPath(String reportsDirectoryPath)
	{
		COSMICUpdateUtil.reportsDirectoryPath = reportsDirectoryPath;
	}
}
