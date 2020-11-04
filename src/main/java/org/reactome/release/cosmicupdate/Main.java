package org.reactome.release.cosmicupdate;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidAttributeException;
import org.reactome.release.common.ReleaseStep;



public class Main extends ReleaseStep
{
	private static String COSMICMutantExport = "./CosmicMutantExport.tsv";
	private static String COSMICFusionExport = "./CosmicFusionExport.tsv";
	private static String COSMICMutationTracking = "./CosmicMutationTracking.tsv";

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
				Main.COSMICMutantExport = props.getProperty("pathToMutantExportFile");
				Main.COSMICFusionExport = props.getProperty("pathToFusionExportFile");
				Main.COSMICMutationTracking = props.getProperty("pathToMutationTrackingFile");
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

	@Override
	public void executeStep(Properties props) throws Exception
	{
		MySQLAdaptor adaptor = ReleaseStep.getMySQLAdaptorFromProperties(props);
		loadTestModeFromProperties(props);
		Collection<GKInstance> cosmicObjects = COSMICUpdateUtil.getCOSMICIdentifiers(adaptor);
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

		Map<String, COSMICIdentifierUpdater> updates = COSMICUpdateUtil.determinePrefixes(filteredCosmicObjects);
		
		COSMICUpdateUtil.validateIdentifiersAgainstFiles(updates, COSMICFusionExport, COSMICMutationTracking, COSMICMutantExport);
		printIdentifierUpdateReport(updates);
		if (!this.testMode)
		{
			updateIdentifiers(adaptor, updates);
		}
	}
	
	/**
	 * Updates the identifiers that need updating.
	 * @param adaptor
	 * @param updates
	 */
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
				// log a message and a full exception with stack trace.
				logger.error("Exception caught while trying to update identifier: "+updator.toString()+" ; Exception is: ",e);
			}
		}		
	}

	/**
	 * Produces a report on identifiers. Report indicates old/"legacy" identifiers, suggested prefixes, new identifiers suggested from COSMIC files,
	 * and validity of old identifiers.
	 * @param updates
	 * @throws IOException
	 */
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

	


}
