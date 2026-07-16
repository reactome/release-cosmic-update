package org.reactome.release.cosmicupdate;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.ReactomeJavaConstants;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.reactome.curation.model.SimpleInstance;

import static org.reactome.release.cosmicupdate.COSMICUpdateUtil.setPersonId;

public class Main {
	private static final Logger logger = LogManager.getLogger();

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

	private Config config;
	private COSMICFileManager cosmicFileManager;

	public static void main(String[] args) throws Exception {
		Main cosmicUpdateStep = new Main();

		JCommander.newBuilder()
			.addObject(cosmicUpdateStep)
			.build()
			.parse(args);

		cosmicUpdateStep.executeStep();

		logger.info("COSMIC Update complete.");
	}

	public void executeStep() throws Exception {
		this.config = new Config(this.configPath);
		this.cosmicFileManager = new COSMICFileManager(this.config);

		redownloadFilesIfTooOld(this.fileAge);

		if (this.executeUpdate) {
			executeUpdate();
		}
	}

	private void redownloadFilesIfTooOld(Duration fileAge) {
		if (fileAge != null) {
			logger.info("User has specified that download process should run.");
			logger.info("Files will be downloaded if they are older than {}", this.fileAge);
			getCosmicFileManager().downloadFiles(fileAge);
		}
	}

	private void executeUpdate() throws Exception {
		logger.info("User has specified that update process should run.");

		try {
			// Step 1: Prepare files
			getCosmicFileManager().unzipFiles();

			// Step 2: Get and filter COSMIC identifiers
			List<SimpleInstance> nonCOSVCosmicDatabaseIdentifierInstances =
				getNonCOSVCosmicDatabaseIdentifierInstances();

			// Step 3: Process and validate identifiers
			setPersonId(config.getPersonId());
			Map<String, List<COSMICIdentifierUpdater>> updaters =
				COSMICUpdateUtil.determinePrefixes(nonCOSVCosmicDatabaseIdentifierInstances);
			validateAndReportUpdates(updaters);

			// Step 4: Perform updates if not in test mode
			if (!getConfig().isTestMode()) {
				updateIdentifiers(updaters);
			}
		} finally {
			// Step 5: Cleanup
			getCosmicFileManager().cleanupFiles();
		}
	}

	private List<SimpleInstance> getNonCOSVCosmicDatabaseIdentifierInstances() {
		List<SimpleInstance> cosmicDatabaseIdentifierInstances =
			COSMICUpdateUtil.getCOSMICDatabaseIdentifierInstances();
		logger.info("{} COSMIC identifiers", cosmicDatabaseIdentifierInstances.size());

		// Filter out COSV prefixes
		List<SimpleInstance> filteredObjects = cosmicDatabaseIdentifierInstances.parallelStream()
			.filter(this::isNotCOSVPrefix)
			.collect(Collectors.toList());

		logger.info("{} filtered COSMIC identifiers", filteredObjects.size());
		return filteredObjects;
	}

	private boolean isNotCOSVPrefix(SimpleInstance instance) {
		try {
			String identifier = (String) instance.getAttribute(ReactomeJavaConstants.identifier);
			return !identifier.toUpperCase().startsWith("COSV");
		} catch (Exception e) {
			throw new RuntimeException("Error accessing instance identifier", e);
		}
	}

	private void validateAndReportUpdates(Map<String, List<COSMICIdentifierUpdater>> updaters) throws Exception {
		COSMICUpdateUtil.validateIdentifiersAgainstFiles(
			updaters,
			getConfig().getFusionExportLocalFilePath(),
			getConfig().getMutationTrackingLocalFilePath(),
			getConfig().getMutantExportLocalFilePath()
		);
		COSMICUpdateUtil.printIdentifierUpdateReport(updaters);
	}

	/**
	 * Updates the identifiers that need updating.
	 * @param updates
	 */
	private void updateIdentifiers(Map<String, List<COSMICIdentifierUpdater>> updates) throws Exception {
		for (COSMICIdentifierUpdater updater : getAllCOSMICIdentifierUpdaters(updates)) {
			updater.updateIdentifier();
		}
	}

	private List<COSMICIdentifierUpdater> getAllCOSMICIdentifierUpdaters(
		Map<String, List<COSMICIdentifierUpdater>> updates) {

		return updates.values().stream().flatMap(List::stream).collect(Collectors.toList());
	}

	private Config getConfig() {
		return this.config;
	}

	private COSMICFileManager getCosmicFileManager() {
		return this.cosmicFileManager;
	}
}
