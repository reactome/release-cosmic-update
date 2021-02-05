package org.reactome.release.cosmicupdate;

import java.io.FileReader;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
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
					+ "Format for a Duration can be found here: https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-"
					+ " Example: PT48H == \"48 hours\"")
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
	private static long personID;

	public static void main(String... args)
	{
		try
		{
			try(Reader configReader = new FileReader("src/main/resources/config.properties"))
			{
				Properties props = new Properties();
				props.load(configReader);
				
				// These default to files in the same directory where the program is running.
				Main.COSMICMutantExport = props.getProperty("pathToMutantExportFile", "./CosmicMutantExport.tsv");
				Main.COSMICFusionExport = props.getProperty("pathToFusionExportFile", "./CosmicFusionExport.tsv");
				Main.COSMICMutationTracking = props.getProperty("pathToMutationTrackingFile", "./CosmicMutationTracking.tsv");
				// These have NO default.
				Main.COSMICUsername = props.getProperty("cosmic.username");
				Main.COSMICPassword = props.getProperty("cosmic.password");
				Main.personID = Long.parseLong(props.getProperty("personID"));
				// These must be set in the properties file.
				Main.COSMICMutantExportURL = props.getProperty("urlToMutantExportFile");
				Main.COSMICFusionExportURL = props.getProperty("urlToFusionExportFile");
				Main.COSMICMutationTrackingURL = props.getProperty("urlToMutationTrackingFile");
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
//			
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
				catch (Exception e)
				{
					// exception caught here means there is probably some fundamental problem with the data
					// such that the program should probably not continue, so throw Error.
					throw new Error(e);
				}
			}).collect(Collectors.toList());
			logger.info("{} filtered COSMIC identifiers", filteredCosmicObjects.size());
	
			Map<String, List<COSMICIdentifierUpdater>> updaters = COSMICUpdateUtil.determinePrefixes(filteredCosmicObjects);
			
			COSMICUpdateUtil.validateIdentifiersAgainstFiles(updaters, COSMICFusionExport, COSMICMutationTracking, COSMICMutantExport);
			COSMICUpdateUtil.printIdentifierUpdateReport(updaters);
			if (!this.testMode)
			{
				updateIdentifiers(adaptor, updaters);
			}
		}
	}

	/**
	 * Checks that a config value. If it is null or blank or empty,
	 * an IllegalArgumentException is thrown.
	 * @param value The value to check
	 * @param message The message that will be put into the IllegalArgumentException
	 * @throws IllegalArgumentException If value == null || value.isEmpty()
	 */
	private void validateConfigValue(String value, String message)
	{
		if (value == null || value.trim().isEmpty())
		{
			throw new IllegalArgumentException(message);
		}
	}
	
	/**
	 * Download the data files from COSMIC.
	 * @throws Exception
	 */
	private void downloadFiles() throws Exception
	{
		//TODO: Better Properties class in release-common-lib. (for future work)
		validateConfigValue(Main.COSMICUsername, "COSMIC Username cannot be null/empty! Please set a value for cosmic.username in the application's properties file");
		validateConfigValue(Main.COSMICUsername, "COSMIC Password cannot be null/empty! Please set a value for cosmic.password in the application's properties file");
		validateConfigValue(Main.COSMICMutantExportURL, "URL for COSMIC Mutant Export file cannot be null/empty! Please set a value for urlToMutantExportFile in the application's properties file");
		validateConfigValue(Main.COSMICMutationTrackingURL, "URL for COSMIC Mutation Tracking file cannot be null/empty! Please set a value for urlToMutationTrackingFile in the application's properties file");
		validateConfigValue(Main.COSMICFusionExportURL, "URL for COSMIC Fusion Export file cannot be null/empty! Please set a value for urlToFusionExportFile in the application's properties file");
		
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
			
			logDownloadMessage(mutantExportRetriever, mutantExportDestination);
			this.executeDownload(mutantExportRetriever);
			
			logDownloadMessage(mutationTrackingRetriever, mutationTrackingDestination);
			this.executeDownload(mutationTrackingRetriever);
			
			logDownloadMessage(fusionExportRetriever, fusionExportDestination);
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

	private void logDownloadMessage(COSMICFileRetriever retriever, final String destination)
	{
		logger.info("Downloading {} to {}", retriever.getDataURL(), destination);
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
	 * @throws Exception 
	 */
	private static void updateIdentifiers(MySQLAdaptor adaptor, Map<String, List<COSMICIdentifierUpdater>> updates) throws Exception
	{
		for (List<COSMICIdentifierUpdater> updater : updates.values())
		{
			updater.forEach(u -> {
				try
				{
					u.updateIdentfier(adaptor, personID);
				}
				catch (Exception e)
				{
					logger.error("Exception caught while trying to update identifier: " + updater.toString() + " ; Exception is: ", e);
				}
			});
		}
	}
}
