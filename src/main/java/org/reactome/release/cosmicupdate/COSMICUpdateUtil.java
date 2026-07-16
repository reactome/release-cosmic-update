package org.reactome.release.cosmicupdate;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gk.model.ReactomeJavaConstants;
import org.reactome.curation.model.SimpleInstance;

/*
 * This class contains utility methods that are to be used for updating COSMIC identifiers.
 */
public class COSMICUpdateUtil {
	static final String COSMIC_LEGACY_PREFIX = "COSM";
	static final String COSMIC_FUSION_PREFIX = "COSF";

	private static final String COSMIC_FUSION_ID = "FUSION_ID";
	private static final String COSMIC_GENOMIC_MUTATION_ID = "GENOMIC_MUTATION_ID";
	private static final String COSMIC_MUTATION_ID = "MUTATION_ID";
	private static final String COSMIC_LEGACY_MUTATION_ID = "LEGACY_MUTATION_ID";
	private static final Logger logger = LogManager.getLogger();

	private static Report report;

	private static CuratorToolAPI curatorToolAPI;
	private static long personId;

	// Private constructor to prevent instantiation of utility class
	private COSMICUpdateUtil() {
		// ...no-op
	}

	/**
	 * Validate the identifiers in the database by comparing them to the identifiers in the file.
	 * @param updaters A map of updaters (actually, it's a LIST of updaters, in case > 1 object is identified by the
	 *                 same identifier value), keyed by COSMIC identifier.
	 * @param COSMICFusionExportFile The path to the COSMIC Fusion Export file.
	 * @param COSMICMutationTrackingFile The path to the COSMIC Mutation Tracking file.
	 * @param COSMICMutantExportFile The path to the COMSIC Mutant Export file.
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	static void validateIdentifiersAgainstFiles(
		Map<String, List<COSMICIdentifierUpdater>> updaters,
		String COSMICFusionExportFile,
		String COSMICMutationTrackingFile,
		String COSMICMutantExportFile
	) throws IOException, FileNotFoundException {
		// A COSMIC identifier is "valid" if it can be mapped in the COSMIC files.
		// First, process COSF identifiers. A COSMIC Fusion (COSF) identifier is valid if it can be found in the
		// COSMIC Fusion Export file. Pretty simple, right? It gets better, below. ;)
		// 
		// Handle the other COSMIC identifiers (COSM/COSV)
		// Step 1: Look at the COSMIC Mutation Tracking file (a file that maps from old legacy COSM identifiers to new
		// COSV identifiers), and create mappings.
		// Step 2: Look at the COSMIC Mutant Export file (a file with all COSMIC coding point mutations from targeted
		// 	       and genome wide screens from the current release).
		//         IFF an identifier is in Mutant Export, add (to the Updater object) any additional mutations from
		//         Mutant Export, set COSV identifier, and indicate that the identifier is VALID.
		// 
		// The goal is to set valid = true for an identifier that is currently valid in COSMIC - if you search that
		// identifier or create a link containing it, you will get a result. Sometimes, there is no mapping to a
		// COSV identifier for a given COSM identifier. This will result in "valid == false".
		// It may also happen that a mapping from a COSM to a COSV does exist in Mutation Tracking, but the COSM is
		// not in Mutant Export, meaning it is not a *current* identifier in the current COSMIC database, so it will
		// also have "valid == false".
		
		validateAgainstCosmicFusionExportFile(updaters, COSMICFusionExportFile);
		validateAgainstCosmicMutationTrackingFile(updaters, COSMICMutationTrackingFile);
		validateAgainstCosmicMutantExportFile(updaters, COSMICMutantExportFile);
	}

	private static void validateAgainstCosmicFusionExportFile(
		Map<String, List<COSMICIdentifierUpdater>> updaters, String COSMICFusionExportFile) throws IOException {

		Set<String> fusionIDs = getCOSMICFusionIds(updaters);
		logger.info("Now checking with CosmicFusionExport.tsv...");
		try(CSVParser parser = getCSVParser(COSMICFusionExportFile)) {
			parser.forEach( record -> {
				String fusionID = record.get(COSMIC_FUSION_ID);
				if (fusionIDs.contains(fusionID)) {
					// COSF identifiers are valid if they are in the Fusion Export mapping.
					updaters.get(COSMIC_FUSION_PREFIX+fusionID).forEach(updater -> updater.setValid(true));
				}
			});
		}
	}

	private static void validateAgainstCosmicMutationTrackingFile(
		Map<String, List<COSMICIdentifierUpdater>> updaters, String COSMICMutationTrackingFile) throws IOException {

		logger.info("Now checking with CosmicMutationTracking.tsv...");
		// Now we need to look through the HUGE file from COSMIC and see if we can map the identifiers...
		try(CSVParser parser = getCSVParser(COSMICMutationTrackingFile)) {
			parser.forEach(record -> {
				String legacyID = record.get(COSMIC_LEGACY_MUTATION_ID);
				String mutationID = record.get(COSMIC_MUTATION_ID);
				String genomicID = record.get(COSMIC_GENOMIC_MUTATION_ID);
				if (updaters.containsKey(legacyID)) {
					updaters.get(legacyID).forEach(updater -> {
						// It is not yet known if this identifier will be valid as per COSMIC's data.
						updater.addMutationID(mutationID);
						updater.setCosvIdentifier(genomicID);
					});
				}
			});
		}
	}
	
	private static void validateAgainstCosmicMutantExportFile(
		Map<String, List<COSMICIdentifierUpdater>> updaters, String COSMICMutantExportFile) throws IOException {

		logger.info("Now checking with CosmicMutantExport.tsv...");
		try(CSVParser parser = getCSVParser(COSMICMutantExportFile)) {
			parser.forEach(record -> {
				String legacyID = record.get(COSMIC_LEGACY_MUTATION_ID);
				String mutationID = record.get(COSMIC_MUTATION_ID);
				String genomicID = record.get(COSMIC_GENOMIC_MUTATION_ID);
				if (updaters.containsKey(legacyID)) {
					updaters.get(legacyID).forEach(updater -> {
						updater.setValid(true); // only VALID if in MutantExport...
						updater.addMutationID(mutationID);
						updater.setCosvIdentifier(genomicID);
					});
				}
			});
		}
	}

	private static CSVParser getCSVParser(String cosmicFileName) throws IOException {
		return new CSVParser(
			new FileReader(cosmicFileName),
			CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')
		);
	}

	private static Set<String> getCOSMICFusionIds(Map<String, List<COSMICIdentifierUpdater>> updaters) {
		return updaters.keySet()
			.parallelStream()
			.filter(id -> id.toUpperCase().startsWith(COSMIC_FUSION_PREFIX))
			.map(id -> id.toUpperCase().replace(COSMIC_FUSION_PREFIX,""))
			.collect(Collectors.toSet());
	}

	/**
	 * Determines the prefixes for COSMIC identifiers. The rule is:
	 * IF an object has EWASes and there is an EWAS with a FragmentReplacedModification or a
	 * FragmentInsertionModification whose referenceSequence is NOT the referenceEntity of the EWAS... then the
	 * suggested prefix will be COSF (for Fusion), otherwise, COSM is suggested.
	 * @param cosmicObjects Objects that are identified by a COSMIC identifier.
	 * @return A map of <code>COSMICIdentifierUpdater</code>, keyed by COSMIC identifier.
	 * @throws Exception
	 */
	static Map<String, List<COSMICIdentifierUpdater>> determinePrefixes(List<SimpleInstance> cosmicObjects)
		throws Exception {

		Map<String, List<COSMICIdentifierUpdater>> updates = new HashMap<>();

		for (SimpleInstance cosmicObject : cosmicObjects) {
			processCosmicObject(cosmicObject, updates);
		}

		return updates;
	}

	private static void processCosmicObject(
		SimpleInstance cosmicObject,
		Map<String, List<COSMICIdentifierUpdater>> updates
	) throws Exception {
		String identifier = (String) cosmicObject.getAttribute(ReactomeJavaConstants.identifier);

		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier(identifier);
		updater.setCosmicDatabaseIdentifierInstance(cosmicObject);

		List<SimpleInstance> ewases = curatorToolAPI.getReferrers(cosmicObject, ReactomeJavaConstants.crossReference);

		if (ewases == null || ewases.isEmpty()) {
			getReport().printIdentifierWithNoReferrerRecord(identifier);
		} else {
			checkEWASes(identifier, updater, ewases);
		}

		addUpdaterToMap(updates, updater);
	}

	private static void addUpdaterToMap(
		Map<String, List<COSMICIdentifierUpdater>> updates,
		COSMICIdentifierUpdater updater
	) {
		String cosmicIdentifier = COSMICUpdateUtil.stringStartsWithC(updater.getIdentifier())
			? updater.getIdentifier()
			: updater.getSuggestedPrefix() + updater.getIdentifier();

		updates.computeIfAbsent(cosmicIdentifier, k -> new ArrayList<>()).add(updater);
	}


	/**
	 * Checks EWASes to see if they have modifiedResidues that have a referenceSequence that is NOT the same as the
	 * EWASes referenceEntity.
	 * The suggested prefix will be set to COSF on the update record if mismatches are found, otherwise COSM will be
	 * set.
	 * @param identifier Identifier of the object being checked, used for reporting.
	 * @param updater A COSMICIdentifierUpdater whose suggested prefix will be updated.
	 * @param EWASes The EWASes to check. If a non-EWAS is in this list, it will be reported.
	 */
	private static void checkEWASes(String identifier, COSMICIdentifierUpdater updater, List<SimpleInstance> EWASes) {
		for (SimpleInstance potentialEWAS : EWASes) {
			if (!isValidEWAS(potentialEWAS)) {
				getReport().printNonEWASRecord(identifier, potentialEWAS.toString());
				continue;
			}

			potentialEWAS = inflate(potentialEWAS);
			SimpleInstance refSequence = (SimpleInstance) potentialEWAS.getAttribute(ReactomeJavaConstants.referenceEntity);
			refSequence = inflate(refSequence);

			@SuppressWarnings("unchecked")
			List<SimpleInstance> modResidues =
				(List<SimpleInstance>) potentialEWAS.getAttribute(ReactomeJavaConstants.hasModifiedResidue);

			boolean foundMismatchedRefSequence = referenceSequenceMismatchesResidues(refSequence, modResidues);

			String prefix = foundMismatchedRefSequence
				? COSMIC_FUSION_PREFIX
				: COSMIC_LEGACY_PREFIX;

			updater.setSuggestedPrefix(prefix);

			if (foundMismatchedRefSequence) {
				break; // stop processing once a mismatch is found
			}
		}
	}

	private static boolean isValidEWAS(SimpleInstance potentialEWAS) {
		return potentialEWAS.getSchemaClassName().equals(ReactomeJavaConstants.EntityWithAccessionedSequence);
	}


	/**
	 * Checks modifiedResidues (only FragmentReplacedModification and FragmentInsertionModification are of interest) to
	 * see if they match refSequence.
	 * @param refSequence A Reference Sequence
	 * @param modResidues The modified residues.
	 * @return TRUE if there is a mismatch: a mismatch is when the reference sequence DBID != the modifiedResidues'
	 *         referenceSequence's DBID. FALSE, otherwise.
	 */
	private static boolean referenceSequenceMismatchesResidues(SimpleInstance refSequence, List<SimpleInstance> modResidues) {

		long refSequenceId = refSequence.getDbId();

		for (SimpleInstance modResidue : modResidues) {
			String className = modResidue.getSchemaClassName();

			if (className.contains(ReactomeJavaConstants.FragmentReplacedModification) ||
				className.contains(ReactomeJavaConstants.FragmentInsertionModification)) {

				modResidue = inflate(modResidue);
				SimpleInstance residueRefSequence =
					(SimpleInstance) modResidue.getAttribute(ReactomeJavaConstants.referenceSequence);

				if (!residueRefSequence.getDbId().equals(refSequenceId)) {
					return true; // Found a mismatch
				}
			}
		}
		return false; // No mismatches found
	}


	/**
	 * Gets COSMIC identifiers from the graph database. Specifically, all DatabaseIdentifier objects with a
	 * ReferenceDatabase named "COSMIC"
	 * @return A List of DatabaseIdentifier objects.
	 */
	static List<SimpleInstance> getCOSMICDatabaseIdentifierInstances() {
		return curatorToolAPI.getCOSMICDatabaseIdentifiers();
	}

	public static SimpleInstance inflate(SimpleInstance instance) {
		return curatorToolAPI.inflate(instance);
	}
	
	/**
	 * Produces a report on identifiers. Report indicates old/"legacy" identifiers, suggested prefixes, new
	 * identifiers suggested from COSMIC files, and validity of old identifiers.
	 * @param updaters The map of identifier updaters.
	 */
	public static void printIdentifierUpdateReport(Map<String, List<COSMICIdentifierUpdater>> updaters) {
		for (COSMICIdentifierUpdater record : getCosmicRecords(updaters)) {
			getReport().printIdentifierUpdateRecord(getIdentifierUpdateReportLineValues(record));
		}
	}
	
	public static boolean stringStartsWithC(String s) {
		return s.startsWith("C");
	}

	public static void setCuratorToolAPI(CuratorToolAPI curatorToolAPI) {
		COSMICUpdateUtil.curatorToolAPI = curatorToolAPI;
	}

	public static void setPersonId(long personId) {
		COSMICUpdateUtil.personId = personId;
	}

	static long getPersonId() {
		return personId;
	}


	private static List<COSMICIdentifierUpdater> getCosmicRecords(
		Map<String, List<COSMICIdentifierUpdater>> updaters) {

		return updaters.values().parallelStream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
	}

	private static Object[] getIdentifierUpdateReportLineValues(COSMICIdentifierUpdater record) {
		return Arrays.asList(
			record.getCosmicDatabaseIdentifierInstance().getDbId(),
			record.getIdentifier(),
			record.getSuggestedPrefix(),
			record.isValid(),
			record.getCosvIdentifier(),
			record.getMutationIDs().toString(),
			getCosmicSearchURL(record)
		).toArray(new Object[0]);
	}

	private static String getCosmicSearchURL(COSMICIdentifierUpdater record) {
		return "https://cancer.sanger.ac.uk/cosmic/search?q=" + getIdentifierForUrl(record);
	}

	private static String getIdentifierForUrl(COSMICIdentifierUpdater cosmicIdentifierRecord) {
		String identifier = cosmicIdentifierRecord.getIdentifier();

		if (COSMICUpdateUtil.stringStartsWithC(identifier)) {
			return identifier;
		}

		String prefix = cosmicIdentifierRecord.getSuggestedPrefix();
		return (prefix != null) ? prefix + identifier : identifier;
	}

	static void setReport(Report report) {
		COSMICUpdateUtil.report = report;
	}

	private static Report getReport() {
		if (report == null) {
			setReport(new Report());
		}

		return report;
	}
}