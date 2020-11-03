package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
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
	// TODO: Move file path values to config file.
	private static final String COSMIC_MUTANT_EXPORT = "./CosmicMutantExport.tsv";
	private static final String COSMIC_FUSION_EXPORT = "./CosmicFusionExport.tsv";
	private static final String COSMIC_MUTATION_TRACKING = "./CosmicMutationTracking.tsv";

	private static final Logger logger = LogManager.getLogger();
	private static long creatorID;
	public static void main(String... args)
	{
		try
		{
			try(Reader configReader = new FileReader("config.properties"))
			{
				Properties props = new Properties();
				props.load(configReader);
				Main cosmicUpdateStep = new Main();
				cosmicUpdateStep.executeStep(props);
			}
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
		try(CSVParser parser = new CSVParser(new FileReader(COSMIC_FUSION_EXPORT), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')); )
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
		try(CSVParser parser = new CSVParser(new FileReader(COSMIC_MUTATION_TRACKING), CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t'));)
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

	private Map<String, COSMICIdentifierUpdater> determinePrefixes(Collection<GKInstance> cosmicObjects)
		throws InvalidAttributeException, Exception, IOException
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		try(CSVPrinter setPrinter = new CSVPrinter(new FileWriter("CandidateSetsWithCOSMICIdentifiers.csv"), CSVFormat.DEFAULT.withHeader("COSMIC identifier", "Referred-to-by CadidateSet (or other non-EWAS)"));
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
								prefix = foundMismatchedRefSequence ? "COSF" : "COSM" ;
								updateRecord.setSuggestedPrefix(prefix);
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
	public void executeStep(Properties props) throws Exception
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

		Main cosmicUpdater = new Main();
		Map<String, COSMICIdentifierUpdater> updates = cosmicUpdater.determinePrefixes(filteredCosmicObjects);
		
		validateIdentifiersAgainstFiles(updates);
		printIdentifierUpdateReport(updates);
		updateIdentifiers(adaptor, updates);
		
	}
}
