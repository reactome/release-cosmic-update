package org.reactome.release.cosmicupdate;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.reactome.release.common.dataretrieval.cosmic.COSMICFileRetriever;
import org.reactome.util.general.GUnzipCallable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Main extends ReleaseStep
{
	@Parameter(names = {"-u"}, description = "Specifies that the updates should be performed.")
	private boolean executeUpdate;
	
	@Parameter(names = {"-d"}, converter = DurationConverter.class,
			description = "Download the files, if the age of local files exceeds the specified max age, or if the files don't exist."
					+ "Omitting this parameter means download will not occur. Specifying -d with a 0 value will force a download."
					+ "Format for a Duration can be found here: https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-")
	private Duration fileAge;
	
	private static String COSMICMutantExport;
	private static String COSMICFusionExport;
	private static String COSMICMutationTracking;

	private static String COSMICMutantExportURL;
	private static String COSMICFusionExportURL;
	private static String COSMICMutationTrackingURL;
	
	private static String COSMICUsername;
	private static String COSMICPassword;
	
	private static final Logger logger = LogManager.getLogger();
	private static long creatorID;
	
	public static void main(String... args)
	{
		try
		{
			try(Reader configReader = new FileReader("src/main/resources/config.properties"))
			{
				Properties props = new Properties();
				props.load(configReader);
				Main.COSMICMutantExport = props.getProperty("pathToMutantExportFile", "./CosmicMutantExport.tsv");
				Main.COSMICFusionExport = props.getProperty("pathToFusionExportFile", "./CosmicFusionExport.tsv");
				Main.COSMICMutationTracking = props.getProperty("pathToMutationTrackingFile", "./CosmicMutationTracking.tsv");
				Main.COSMICMutantExportURL = props.getProperty("urlToMutantExportFile", "https://cancer.sanger.ac.uk/cosmic/file_download/GRCh38/cosmic/v92/CosmicMutantExport.tsv.gz");
				Main.COSMICFusionExportURL = props.getProperty("urlToFusionExportFile", "https://cancer.sanger.ac.uk/cosmic/file_download/GRCh38/cosmic/v92/CosmicFusionExport.tsv.gz");
				Main.COSMICMutationTrackingURL = props.getProperty("urlToMutationTrackingFile", "https://cancer.sanger.ac.uk/cosmic/file_download/GRCh38/cosmic/v92//CosmicMutationTracking.tsv.gz");
				Main.COSMICUsername = props.getProperty("cosmic.username");
				Main.COSMICPassword = props.getProperty("cosmic.password");
				Main cosmicUpdateStep = new Main();
				JCommander.newBuilder().addObject(cosmicUpdateStep).build().parse(args);
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
		if (this.fileAge != null)
		{
			logger.info("User has specified that download process should run.");
			logger.info("Files will be downloaded if they are older than {}", this.fileAge);
			this.downloadFiles();
		}
		
		if (this.executeUpdate)
		{
			logger.info("User has specified that update process should run.");
			// first thing, check the files and unzip them if necessary.
			GUnzipCallable unzipper1 = new GUnzipCallable(Paths.get(Main.COSMICFusionExport + ".gz"), Paths.get(Main.COSMICFusionExport));
			GUnzipCallable unzipper2 = new GUnzipCallable(Paths.get(Main.COSMICMutantExport + ".gz"), Paths.get(Main.COSMICMutantExport));
			GUnzipCallable unzipper3 = new GUnzipCallable(Paths.get(Main.COSMICMutationTracking + ".gz"), Paths.get(Main.COSMICMutationTracking));
			
			ExecutorService execService = Executors.newCachedThreadPool();
			// The files are large and it could be slow to unzip them sequentially, so we will unzip them in parallel.
			execService.invokeAll(Arrays.asList(unzipper1, unzipper2, unzipper3));
			execService.shutdown();
			
			MySQLAdaptor adaptor = ReleaseStep.getMySQLAdaptorFromProperties(props);
			loadTestModeFromProperties(props);
			this.downloadFiles();
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
	}

	/**
	 * Download the data files from COSMIC.
	 * @throws Exception
	 */
	private void downloadFiles() throws Exception
	{
		COSMICFileRetriever mutantExportRetriever = new COSMICFileRetriever();
		COSMICFileRetriever mutationTrackingRetriever = new COSMICFileRetriever();
		COSMICFileRetriever fusionExportRetriever = new COSMICFileRetriever();
		
		try
		{
			mutantExportRetriever.setDataURL(new URI(Main.COSMICMutantExportURL));
			mutationTrackingRetriever.setDataURL(new URI(Main.COSMICMutationTrackingURL));
			fusionExportRetriever.setDataURL(new URI(Main.COSMICFusionExportURL));
			
			final String mutantExportDestination = "./CosmicMutantExport.tsv.gz";
			final String mutationTrackingDestination = "./CosmicMutationTracking.tsv.gz";
			final String fusionExportDestination = "./CosmicFusionExport.tsv.gz";
			
			mutantExportRetriever.setFetchDestination(mutantExportDestination);
			mutationTrackingRetriever.setFetchDestination(mutationTrackingDestination);
			fusionExportRetriever.setFetchDestination(fusionExportDestination);
			
			logger.info("Downloading {} to {}", mutantExportRetriever.getDataURL(), mutantExportDestination);
			this.executeDownload(mutantExportRetriever);
			
			logger.info("Downloading {} to {}", mutationTrackingRetriever.getDataURL(), mutationTrackingDestination);
			this.executeDownload(mutationTrackingRetriever);
			
			logger.info("Downloading {} to {}", fusionExportRetriever.getDataURL(), fusionExportDestination);
			this.executeDownload(fusionExportRetriever);
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
			throw e;
		}
		catch (Exception e)
		{
			logger.error("Error occurred while trying to download a file! " + e.getMessage(), e);
			// throw the exception further up the stack - can't continue if file downloads are failing.
			throw e;
		}
	}
	
	/**
	 * Executes a single download.
	 * @param retriever - the file retriever to run.
	 * @throws Exception
	 */
	private void executeDownload(COSMICFileRetriever retriever) throws Exception
	{
		retriever.setMaxAge(this.fileAge);
		retriever.setUserName(Main.COSMICUsername);
		retriever.setPassword(Main.COSMICPassword);
		retriever.fetchData();
	}
	
	/**
	 * Updates the identifiers that need updating.
	 * @param adaptor
	 * @param updates
	 */
	private static void updateIdentifiers(MySQLAdaptor adaptor, Map<String, COSMICIdentifierUpdater> updates)
	{
		for (COSMICIdentifierUpdater updater : updates.values())
		{
			try
			{
				updater.updateIdentfier(adaptor, creatorID);
			}
			catch (Exception e)
			{
				// log a message and a full exception with stack trace.
				logger.error("Exception caught while trying to update identifier: "+updater.toString()+" ; Exception is: ",e);
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
		try(CSVPrinter printer = new CSVPrinter(new FileWriter("./COSMIC-identifiers-report.csv"), CSVFormat.DEFAULT.withHeader("DB_ID", "Identifier", "Suggested Prefix", "Valid?", "COSV identifier", "Mutation IDs")))
		{
			for (COSMICIdentifierUpdater record : updates.values().parallelStream().sorted().collect(Collectors.toList()))
			{
				printer.printRecord(record.getDbID(), record.getIdentifier(), record.getSuggestedPrefix(), record.isValid(), record.getCosvIdentifier(), record.getMutationIDs().toString());
			}
		}
	}
}
