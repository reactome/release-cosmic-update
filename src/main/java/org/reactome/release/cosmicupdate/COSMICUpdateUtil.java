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
	private static final String COSMIC_FUSION_PREFIX = "COSF";
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * 
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
				String fusionID = record.get("FUSION_ID");
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
				String legacyID = record.get("LEGACY_MUTATION_ID");
				String mutationID = record.get("MUTATION_ID");
				String genomicID = record.get("GENOMIC_MUTATION_ID");
				if (updates.containsKey(legacyID))
				{
					updates.get(legacyID).getMutationIDs().add(mutationID);
					updates.get(legacyID).setCosvIdentifier(genomicID);
				}
			
			});
		}
		logger.info("Now checking with CosmicMutantExport.tsv...");
		try(CSVParser parser = new CSVParser(new FileReader(COSMICMutantExportFile), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));
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

	/**
	 * 
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
								prefix = foundMismatchedRefSequence ? COSMIC_FUSION_PREFIX : "COSM";
								updateRecord.setSuggestedPrefix(prefix);
							}
							else
							{
								nonEWASPrinter.printRecord(identifier, ewas.toString());
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
			logger.info("Too many COSMIC refDBs: {}", refDBs.size());
			logger.info("Cannot proceeed!");
			System.exit(1);
		}
		
		@SuppressWarnings("unchecked")
		Collection<GKInstance> cosmicObjects = adaptor.fetchInstanceByAttribute(ReactomeJavaConstants.DatabaseIdentifier, ReactomeJavaConstants.referenceDatabase, " = ", cosmicRefDB.getAttributeValue(ReactomeJavaConstants.DB_ID));
		return cosmicObjects;
	}
}
