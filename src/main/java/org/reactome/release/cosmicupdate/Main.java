package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.gk.schema.InvalidAttributeException;
import org.reactome.release.common.ReleaseStep;
import org.reactome.release.common.dataretrieval.cosmic.COSMICFileRetriever;
import org.reactome.util.general.DBUtils;
import org.reactome.util.general.GUnzipCallable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Main extends ReleaseStep {
	@Parameter(names = {"-u"}, description = "Specifies that the updates should be performed.")
	private boolean executeUpdate;

	@Parameter(names = {"-d"}, converter = DurationConverter.class,
		description = "Download the files, if the age of local files exceeds the specified max age, or if the files"
			+ " don't exist.  Omitting this parameter means download will not occur. Specifying -d with a 0 value"
			+ " will force a download.  Format for a Duration can be found here:"
			+ " https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-"
			+ " Example: PT48H == \"48 hours\"")
	private Duration fileAge;

	@Parameter(names = {"-c"},
		description = "The path to the configuration file. Default is src/main/resources/config.properties")
	private String configPath = "src/main/resources/config.properties";

	//TODO: Create a separate properties file for authentication values (username, password).
//	@Parameter(names = {"-a"},
//		description = "The path to the authentication file, containing usernames and passwords.
//			+ Default is src/main/resources/auth.properties")
//	private static String authPath = "src/main/resources/auth.properties";

	private static String COSMICMutantExport;
	private static String COSMICFusionExport;
	private static String COSMICMutationTracking;

	private static String COSMICMutantExportURL;
	private static String COSMICFusionExportURL;
	private static String COSMICMutationTrackingURL;

	private static String COSMICUsername;
	private static String COSMICPassword;

	private static final Logger logger = LogManager.getLogger();
	private static long personId;

	public static void main(String[] args) throws Exception {
		Main cosmicUpdateStep = new Main();

		JCommander.newBuilder()
			.addObject(cosmicUpdateStep)
			.build()
			.parse(args);
		Properties configProps = cosmicUpdateStep.getConfig();
		cosmicUpdateStep.executeStep(configProps);

		logger.info("COSMIC Update complete.");
	}

	private Properties getConfig() {
		boolean noConfigSpecified = this.configPath != null && this.configPath.trim().isEmpty();
//		boolean noAuthConfigSpecified = Main.authPath != null && !Main.authPath.trim().isEmpty();

		if (noConfigSpecified) {
			String errorMessage = "An empty path to config.properties was given.  " +
				"Please give a valid path with -c at command line or use default.";
			logger.fatal(errorMessage);
			throw new RuntimeException(errorMessage);
		}
//		if (noAuthConfigSpecified) {
//			logger.fatal("Empty/null value for path to auth.properties file was specified. This MUST be specified
//				as \"-c /path/to/auth.properties\" "
//					+ "or do not specify anything and the default will be src/main/resources/auth.properties");
//		throw new RuntimeException("Empty/null values were given for the path to an auth file. The program " +
//		"cannot run without the auth file.");
//		}
		Properties configProps = getConfigProperties();

		// These default to files in the same directory where the program is running.
		Main.COSMICMutantExport =
			configProps.getProperty("pathToMutantExportFile", "./CosmicMutantExport.tsv");
		Main.COSMICFusionExport =
			configProps.getProperty("pathToFusionExportFile", "./CosmicFusionExport.tsv");
		Main.COSMICMutationTracking =
			configProps.getProperty("pathToMutationTrackingFile", "./CosmicMutationTracking.tsv");

		// These have NO default.
		Main.COSMICUsername = configProps.getProperty("cosmic.user");
		Main.COSMICPassword = configProps.getProperty("cosmic.password");
		Main.personId = Long.parseLong(configProps.getProperty("personId"));

		// These must be set in the properties file.
		Main.COSMICMutantExportURL = configProps.getProperty("urlToMutantExportFile");
		Main.COSMICFusionExportURL = configProps.getProperty("urlToFusionExportFile");
		Main.COSMICMutationTrackingURL = configProps.getProperty("urlToMutationTrackingFile");

		return configProps;
	}

	@Override
	public void executeStep(Properties props) throws Exception {
		redownloadFilesIfTooOld(this.fileAge);

		if (this.executeUpdate) {
			executeUpdate(props);
		}
	}

	private Properties getConfigProperties() {
		Properties configProps = new Properties();

		try(Reader configReader = new FileReader(this.configPath)) {
			configProps.load(configReader);
		} catch (IOException e) {
			throw new RuntimeException("Unable to load config file", e);
		}
		return configProps;
	}

	private void redownloadFilesIfTooOld(Duration fileAge) throws Exception {
		if (fileAge != null) {
			logger.info("User has specified that download process should run.");
			logger.info("Files will be downloaded if they are older than {}", this.fileAge);
			this.downloadFiles();
		}
	}

	private void executeUpdate(Properties props) throws Exception {
		logger.info("User has specified that update process should run.");

		unzipFiles();

		MySQLAdaptor adaptor = DBUtils.getCuratorDbAdaptor(props);
		Collection<GKInstance> cosmicObjects = COSMICUpdateUtil.getCOSMICIdentifiers(adaptor);
		logger.info("{} COSMIC identifiers", cosmicObjects.size());
		// Filter the identifiers to exclude the COSV prefixes.
		List<GKInstance> filteredCosmicObjects = cosmicObjects.parallelStream().filter(inst -> {
			try {
				return !((String)inst.getAttributeValue(ReactomeJavaConstants.identifier)).toUpperCase().startsWith("COSV");
			} catch (Exception e) {
				// exception caught here means there is probably some fundamental problem with the data
				// such that the program should probably not continue..
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());
		logger.info("{} filtered COSMIC identifiers", filteredCosmicObjects.size());

		Map<String, List<COSMICIdentifierUpdater>> updaters = COSMICUpdateUtil.determinePrefixes(filteredCosmicObjects);

		COSMICUpdateUtil.validateIdentifiersAgainstFiles(updaters, COSMICFusionExport, COSMICMutationTracking, COSMICMutantExport);
		COSMICUpdateUtil.printIdentifierUpdateReport(updaters);

		loadTestModeFromProperties(props);
		if (!this.testMode) {
			updateIdentifiers(adaptor, updaters);
		}
		cleanupFiles();
	}

	private void unzipFiles() throws InterruptedException {
		ExecutorService execService = Executors.newCachedThreadPool();
		// The files are large and it could be slow to unzip them sequentially, so we will unzip them in parallel.
		execService.invokeAll(getGUnzipCallables());
		execService.shutdown();
	}

	private List<GUnzipCallable> getGUnzipCallables() {
		return Stream.of(COSMICFusionExport, COSMICMutantExport, COSMICMutationTracking)
			.map(this::getGUnzipCallable)
			.collect(Collectors.toList());
	}

	private GUnzipCallable getGUnzipCallable(String filePathAsString) {
		return new GUnzipCallable(getGZippedFilePath(filePathAsString), getGUnzippedFilePath(filePathAsString));
	}

	private Path getGZippedFilePath(String filePathAsString) {
		return Paths.get(getGUnzippedFilePath(filePathAsString).toString() + ".gz");
	}

	private Path getGUnzippedFilePath(String filePathAsString) {
		return Paths.get(filePathAsString);
	}

	/**
	 * Clean up the uncompressed data files. Don't remove the zipped files, because if you need to run this code again,
	 * you'll be stuck waiting for the files to download again. Try to avoid removing the zipped files until you're *sure*
	 * this step has run successfully.
	 */
	private void cleanupFiles() {
		List<String> filesToDelete = Arrays.asList(COSMICFusionExport, COSMICMutantExport, COSMICMutationTracking);
		for (String file : filesToDelete) {
			deleteFile(file);
		}
	}

	/**
	 * Deletes a file and logs a message about the deletion.
	 * @param fileName
	 */
	private void deleteFile(String fileName) {
		try {
			Files.deleteIfExists(Paths.get(fileName));
			logger.info("{} was deleted.", fileName);
		} catch (IOException e) {
			logger.warn("IOException caught while cleaning up the data file: \"" + fileName + "\". " +
				"You may need to manually remove any remaining files.", e);
		}
	}

	/**
	 * Checks that a config value. If it is null or blank or empty,
	 * an IllegalArgumentException is thrown.
	 * @param value The value to check
	 * @param message The message that will be put into the IllegalArgumentException
	 * @throws IllegalArgumentException If value == null || value.isEmpty()
	 */
	private void validateConfigValue(String value, String message) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Download the data files from COSMIC.
	 * @throws Exception
	 */
	private void downloadFiles() throws IllegalArgumentException, URISyntaxException, Exception {
		//TODO: Better Properties class in release-common-lib. (for future work)
		validateConfigValue(Main.COSMICUsername,
			"COSMIC Username cannot be null/empty! Please set a value for cosmic.username in the " +
				"application's properties file");
		validateConfigValue(Main.COSMICUsername,
			"COSMIC Password cannot be null/empty! Please set a value for cosmic.password in the " +
				"application's properties file");
		validateConfigValue(Main.COSMICMutantExportURL,
			"URL for COSMIC Mutant Export file cannot be null/empty! Please set a value for " +
				"urlToMutantExportFile in the application's properties file");
		validateConfigValue(Main.COSMICMutationTrackingURL,
			"URL for COSMIC Mutation Tracking file cannot be null/empty! Please set a value for " +
				"urlToMutationTrackingFile in the application's properties file");
		validateConfigValue(Main.COSMICFusionExportURL, "URL for COSMIC Fusion Export file cannot be null/empty!" +
			"  Please set a value for urlToFusionExportFile in the application's properties file");

		COSMICFileRetriever mutantExportRetriever = new COSMICFileRetriever();
		COSMICFileRetriever mutationTrackingRetriever = new COSMICFileRetriever();
		COSMICFileRetriever fusionExportRetriever = new COSMICFileRetriever();

		try {
			mutantExportRetriever.setDataURL(new URI(Main.COSMICMutantExportURL));
			mutationTrackingRetriever.setDataURL(new URI(Main.COSMICMutationTrackingURL));
			fusionExportRetriever.setDataURL(new URI(Main.COSMICFusionExportURL));

			Properties configProps = getConfigProperties();

			final String mutantExportDestination = addGzipExtension(configProps.getProperty(
				"pathToMutantExportFile", "./CosmicMutantExport.tsv"));
			final String mutationTrackingDestination = addGzipExtension(configProps.getProperty(
				"pathToMutationTrackingFile", "./CosmicMutationTracking.tsv"));
			final String fusionExportDestination = addGzipExtension(configProps.getProperty(
				"pathToFusionExportFile", "./CosmicFusionExport.tsv"));

			mutantExportRetriever.setFetchDestination(mutantExportDestination);
			mutationTrackingRetriever.setFetchDestination(mutationTrackingDestination);
			fusionExportRetriever.setFetchDestination(fusionExportDestination);

			logDownloadMessage(mutantExportRetriever, mutantExportDestination);
			this.executeDownload(mutantExportRetriever);

			logDownloadMessage(mutationTrackingRetriever, mutationTrackingDestination);
			this.executeDownload(mutationTrackingRetriever);

			logDownloadMessage(fusionExportRetriever, fusionExportDestination);
			this.executeDownload(fusionExportRetriever);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			logger.error("Error occurred while trying to download a file! " + e.getMessage(), e);
			// throw the exception further up the stack - can't continue if file downloads are failing.
			throw e;
		}
	}

	private void logDownloadMessage(COSMICFileRetriever retriever, final String destination) {
		logger.info("Downloading {} to {}", retriever.getDataURL(), destination);
	}

	/**
	 * Executes a single download.
	 * @param retriever - the file retriever to run.
	 * @throws Exception
	 */
	private void executeDownload(COSMICFileRetriever retriever) throws Exception {
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
	private static void updateIdentifiers(MySQLAdaptor adaptor, Map<String, List<COSMICIdentifierUpdater>> updates)
		throws Exception {

		for (List<COSMICIdentifierUpdater> listOfUpdaters : updates.values()) {
			listOfUpdaters.forEach(updater -> {
				try {
					updater.updateIdentfier(adaptor, personId);
				} catch (Exception e) {
					logger.error("Exception caught while trying to update identifier: " +
						listOfUpdaters.toString() + " ; Exception is: ", e);
				}
			});
		}
	}

	private String addGzipExtension(String filePath) {
		return filePath + ".gz";
	}
}
