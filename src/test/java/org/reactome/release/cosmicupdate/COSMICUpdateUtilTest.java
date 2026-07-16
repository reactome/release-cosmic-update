package org.reactome.release.cosmicupdate;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gk.model.ReactomeJavaConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactome.curation.model.SimpleInstance;

public class COSMICUpdateUtilTest {

	private static final String IDENTIFIER_COSF1234 = "COSF1234";
	private static final String IDENTIFIER_1234 = "1234";
	private static final long DBID_1235 = 1235L;
	private static final long DBID_1234 = 1234L;
	private static final String IDENTIFIER_COSM1111 = "COSM1111";
	private static final String IDENTIFIER_44444 = "44444";
	private static final String IDENTIFIER_5678 = "5678";
	private static final String IDENTIFIER_COSF6321 = "COSF6321";
	private static final String IDENTIFIER_COSF7890 = "COSF7890";
	private String COSMICFusionExportFile = null;
	private String COSMICMutationTrackingFile = null;
	private String COSMICMutantExportFile = null;
	
	private static final String COSMIC_GENOMIC_MUTATION_ID = "GENOMIC_MUTATION_ID";
	private static final String COSMIC_MUTATION_ID = "MUTATION_ID";
	private static final String COSMIC_LEGACY_MUTATION_ID = "LEGACY_MUTATION_ID";

	private static final String MUTANT_HEADER =  COSMIC_LEGACY_MUTATION_ID + "\t" +  COSMIC_MUTATION_ID+ "\t" + COSMIC_GENOMIC_MUTATION_ID + "\n"; 
	
	@Before
	public void setup() throws IOException {
		// need to create temp files to read.
		createTestCosmicFusionExport();
		createTestCosmicMutantExport();
		createTestCosmicMutationTracking();
		
	}


	private void createTestCosmicMutationTracking() throws IOException {
		Path pathToMutationTracking = Files.createTempFile("mutationTracking", "csv");
		this.COSMICMutationTrackingFile = pathToMutationTracking.toString();
		Files.writeString(pathToMutationTracking, MUTANT_HEADER);
		Files.writeString(pathToMutationTracking, "1234\t1234\tCOSV2323232\n", StandardOpenOption.APPEND);
		
	}


	private void createTestCosmicMutantExport() throws IOException {
		Path pathToMutantExport = Files.createTempFile("mutantExport", "csv");
		this.COSMICMutantExportFile = pathToMutantExport.toString();
		Files.writeString(pathToMutantExport, MUTANT_HEADER);
		Files.writeString(pathToMutantExport, "1234\t1234\tCOSV2323232\n", StandardOpenOption.APPEND);
		
	}


	private void createTestCosmicFusionExport() throws IOException {
		Path pathToCOSF = Files.createTempFile("COSF", "csv");
		this.COSMICFusionExportFile = pathToCOSF.toString();
		// we only use the FUSION_ID field from the COSMIC Fusion Export.
		Files.writeString(pathToCOSF, "FUSION_ID\n");
		Files.writeString(pathToCOSF, IDENTIFIER_1234+"\n", StandardOpenOption.APPEND);
		Files.writeString(pathToCOSF, IDENTIFIER_COSF6321+"\n", StandardOpenOption.APPEND);
		Files.writeString(pathToCOSF, IDENTIFIER_COSF7890+"\n", StandardOpenOption.APPEND);
	}
	
	
	@Test
	public void testValidateAgainstFiles() {
		Map<String, List<COSMICIdentifierUpdater>> updates = new HashMap<>();
		
		COSMICIdentifierUpdater updater1 = new COSMICIdentifierUpdater();
		COSMICIdentifierUpdater updater2 = new COSMICIdentifierUpdater();
		updates.put(IDENTIFIER_1234, Arrays.asList(updater1));
		updates.put(IDENTIFIER_COSF1234, Arrays.asList(updater1));
		updates.put(IDENTIFIER_5678, Arrays.asList(updater2));
		
		try {
			COSMICUpdateUtil.validateIdentifiersAgainstFiles(updates, COSMICFusionExportFile, COSMICMutationTrackingFile, COSMICMutantExportFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}		
	}
	
	@Test
	public void testDeterminePrefixes() throws Exception {
		List<SimpleInstance> cosmicObjects = new ArrayList<>();
		List<SimpleInstance> EWASes = new ArrayList<>();
		List<SimpleInstance> mockModResidues = new ArrayList<>();

		CuratorToolAPI curatorToolAPI = Mockito.mock(CuratorToolAPI.class);
		SimpleInstance mockCosmicObject = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockCosmicObject2 = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockCosmicObject3 = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockEWAS = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockNonEWAS = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockRefSeq = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockRefSeq2 = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockModResidue = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockModResidue2 = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockResiduRefSeq = Mockito.mock(SimpleInstance.class);
		SimpleInstance mockResiduRefSeq2 = Mockito.mock(SimpleInstance.class);
		
		// set up mock object - will not have any EWASes
		Mockito.when(mockCosmicObject.getAttribute(ReactomeJavaConstants.identifier)).thenReturn(IDENTIFIER_5678);
		Mockito.when(mockCosmicObject2.getAttribute(ReactomeJavaConstants.identifier)).thenReturn(IDENTIFIER_44444);
		// object 3 has no EWASes
		Mockito.when(mockCosmicObject3.getAttribute(ReactomeJavaConstants.crossReference)).thenReturn(null);
		Mockito.when(mockCosmicObject3.getAttribute(ReactomeJavaConstants.identifier)).thenReturn(IDENTIFIER_COSM1111);
		
		// set up an EWAS for second mock object
		Mockito.when(mockEWAS.getSchemaClassName()).thenReturn(ReactomeJavaConstants.EntityWithAccessionedSequence);
		Mockito.when(mockRefSeq.getDbId()).thenReturn(DBID_1234);
		// mismatch here on DBID should trigger a COSF prefix suggestion.
		Mockito.when(mockRefSeq2.getDbId()).thenReturn(DBID_1235);
		Mockito.when(mockEWAS.getAttribute(ReactomeJavaConstants.referenceEntity)).thenReturn(mockRefSeq).thenReturn(mockRefSeq2);
		// need modified residues
		Mockito.when(mockModResidue.getSchemaClassName()).thenReturn(ReactomeJavaConstants.FragmentReplacedModification);
		Mockito.when(mockModResidue.getDbId()).thenReturn(DBID_1234);
		Mockito.when(mockModResidue2.getSchemaClassName()).thenReturn(ReactomeJavaConstants.FragmentInsertionModification);
		Mockito.when(mockModResidue2.getDbId()).thenReturn(DBID_1234);
		Mockito.when(mockResiduRefSeq.getDbId()).thenReturn(DBID_1234);
		Mockito.when(mockResiduRefSeq2.getDbId()).thenReturn(DBID_1234);
		Mockito.when(mockModResidue.getAttribute(ReactomeJavaConstants.referenceSequence)).thenReturn(mockResiduRefSeq);
		Mockito.when(mockModResidue2.getAttribute(ReactomeJavaConstants.referenceSequence)).thenReturn(mockResiduRefSeq2);
		Mockito.when(mockEWAS.getAttribute(ReactomeJavaConstants.hasModifiedResidue)).thenReturn(mockModResidues);

		mockModResidues.add(mockModResidue2);
		mockModResidues.add(mockModResidue);
		
		// set up an non-EWAS
		Mockito.when(mockNonEWAS.getSchemaClassName()).thenReturn("NOT_AN_EWAS");

		// put EWAS and non-EWAS in list of EWASes
		EWASes.add(mockNonEWAS);
		EWASes.add(mockEWAS);
		
		// set up EWASes
		Mockito.when(curatorToolAPI.getReferrers(mockCosmicObject2, ReactomeJavaConstants.crossReference)).thenReturn(EWASes);
		Mockito.when(curatorToolAPI.getReferrers(mockCosmicObject, ReactomeJavaConstants.crossReference)).thenReturn(EWASes);
		
		// add both mocks to list
		cosmicObjects.add(mockCosmicObject);
		cosmicObjects.add(mockCosmicObject2);
		cosmicObjects.add(mockCosmicObject3);

		Path reportPath = Files.createTempDirectory("cosmic_update");
		System.out.println("Reports will be in " + reportPath.toAbsolutePath().toString());
		COSMICUpdateUtil.setReport(new Report(reportPath.toAbsolutePath().toString()));

		// test with list
		Map<String, List<COSMICIdentifierUpdater>> result = COSMICUpdateUtil.determinePrefixes(cosmicObjects);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		
		for (String s : result.keySet()) {
			System.out.println(s + "\t" + result.get(s).toString());
			if (s.contains(IDENTIFIER_44444)) {
				// The 44444 COSMIC object should get COSF because of mismatch in one of the modified residues.
				assertTrue(result.get(s).get(0).getSuggestedPrefix().equals("COSF"));
			}
			if (s.contains(IDENTIFIER_5678)) {
				// This should have a COSM prefix.
				assertTrue(result.get(s).get(0).getSuggestedPrefix().equals("COSM"));
			}
		}
		COSMICUpdateUtil.printIdentifierUpdateReport(result);
		boolean identifersReportExists = false;
		boolean noReferrersReportExists = false;
		boolean nonEWASReportExists = false;
		DirectoryStream<Path> dirStream = Files.newDirectoryStream(reportPath);
		
		for (Path p : dirStream) {
			String fileName = p.getFileName().toString();
			if (fileName.endsWith(".csv")) {
				if (fileName.startsWith("COSMIC-identifiers-report")) {
					identifersReportExists = true;
				} else if (fileName.startsWith("COSMICIdentifiersNoReferrers")) {
					noReferrersReportExists = true;
				} else if (fileName.startsWith("nonEWASObjectsWithCOSMICIdentifiers")) {
					nonEWASReportExists = true;
				}
			}
		}
		assertTrue(identifersReportExists);
		assertTrue(noReferrersReportExists);
		assertTrue(nonEWASReportExists);
	}
}