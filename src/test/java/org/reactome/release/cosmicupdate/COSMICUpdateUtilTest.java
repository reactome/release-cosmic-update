package org.reactome.release.cosmicupdate;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.schema.InvalidAttributeException;
import org.gk.schema.SchemaClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class COSMICUpdateUtilTest
{

	private String COSMICFusionExportFile = null;
	private String COSMICMutationTrackingFile = null;
	private String COSMICMutantExportFile = null;
	
	private static final String COSMIC_GENOMIC_MUTATION_ID = "GENOMIC_MUTATION_ID";
	private static final String COSMIC_MUTATION_ID = "MUTATION_ID";
	private static final String COSMIC_LEGACY_MUTATION_ID = "LEGACY_MUTATION_ID";

	private static final String MUTANT_HEADER =  COSMIC_LEGACY_MUTATION_ID + "\t" +  COSMIC_MUTATION_ID+ "\t" + COSMIC_GENOMIC_MUTATION_ID + "\n"; 
	
	@Before
	public void setup() throws IOException
	{
		// need to create temp files to read.
		createTestCosmicFusionExport();
		createTestCosmicMutantExport();
		createTestCosmicMutationTracking();
		
	}


	private void createTestCosmicMutationTracking() throws IOException
	{
		Path pathToMutationTracking = Files.createTempFile("mutationTracking", "csv");
		this.COSMICMutationTrackingFile = pathToMutationTracking.toString();
		Files.writeString(pathToMutationTracking, MUTANT_HEADER);
		Files.writeString(pathToMutationTracking, "1234\t1234\tCOSV2323232\n", StandardOpenOption.APPEND);
		
	}


	private void createTestCosmicMutantExport() throws IOException
	{
		Path pathToMutantExport = Files.createTempFile("mutantExport", "csv");
		this.COSMICMutantExportFile = pathToMutantExport.toString();
		Files.writeString(pathToMutantExport, MUTANT_HEADER);
		Files.writeString(pathToMutantExport, "1234\t1234\tCOSV2323232\n", StandardOpenOption.APPEND);
		
	}


	private void createTestCosmicFusionExport() throws IOException
	{
		Path pathToCOSF = Files.createTempFile("COSF", "csv");
		this.COSMICFusionExportFile = pathToCOSF.toString();
		// we only use the FUSION_ID field from the COSMIC Fusion Export.
		Files.writeString(pathToCOSF, "FUSION_ID\n");
		Files.writeString(pathToCOSF, "1234\n", StandardOpenOption.APPEND);
		Files.writeString(pathToCOSF, "COSF6321\n", StandardOpenOption.APPEND);
		Files.writeString(pathToCOSF, "COSF7890\n", StandardOpenOption.APPEND);
	}
	
	
	@Test
	public void testValidateAgainstFiles()
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		
		COSMICIdentifierUpdater updater1 = new COSMICIdentifierUpdater();
		COSMICIdentifierUpdater updater2 = new COSMICIdentifierUpdater();
		updates.put("1234", updater1);
		updates.put("COSF1234", updater1);
		updates.put("5678", updater2);
		
		try
		{
			COSMICUpdateUtil.validateIdentifiersAgainstFiles(updates, COSMICFusionExportFile, COSMICMutationTrackingFile, COSMICMutantExportFile);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			fail();
		}		
	}
	
	@Test
	public void testDeterminePrefixes() throws InvalidAttributeException, IOException, Exception
	{
		Collection<GKInstance> cosmicObjects = new ArrayList<>();
		Collection<GKInstance> EWASes = new ArrayList<>();
		List<GKInstance> mockModResidues = new ArrayList<>();
		
		GKInstance mockCosmicObject = Mockito.mock(GKInstance.class);
		GKInstance mockCosmicObject2 = Mockito.mock(GKInstance.class);
		GKInstance mockCosmicObject3 = Mockito.mock(GKInstance.class);
		GKInstance mockEWAS = Mockito.mock(GKInstance.class);
		GKInstance mockNonEWAS = Mockito.mock(GKInstance.class);
		GKInstance mockRefSeq = Mockito.mock(GKInstance.class);
		GKInstance mockRefSeq2 = Mockito.mock(GKInstance.class);
		GKInstance mockModResidue = Mockito.mock(GKInstance.class);
		GKInstance mockModResidue2 = Mockito.mock(GKInstance.class);
		GKInstance mockResiduRefSeq = Mockito.mock(GKInstance.class);
		GKInstance mockResiduRefSeq2 = Mockito.mock(GKInstance.class);
		SchemaClass mockEWASSchemaClass = Mockito.mock(SchemaClass.class);
		SchemaClass mockNonEWASSchemaClass = Mockito.mock(SchemaClass.class);
		SchemaClass mockFragReplaceSchemaClass = Mockito.mock(SchemaClass.class);
		SchemaClass mockFragInsertSchemaClass = Mockito.mock(SchemaClass.class);
		
		// set up mock object - will not have any EWASes
		Mockito.when(mockCosmicObject.getAttributeValue(ReactomeJavaConstants.identifier)).thenReturn("5678");
		Mockito.when(mockCosmicObject2.getAttributeValue(ReactomeJavaConstants.identifier)).thenReturn("44444");
		// object 3 has no EWASes
		Mockito.when(mockCosmicObject3.getAttributeValue(ReactomeJavaConstants.crossReference)).thenReturn(null);
		Mockito.when(mockCosmicObject3.getAttributeValue(ReactomeJavaConstants.identifier)).thenReturn("COSM1111");
		
		// set up an EWAS for second mock object
		Mockito.when(mockEWASSchemaClass.getName()).thenReturn(ReactomeJavaConstants.EntityWithAccessionedSequence);
		Mockito.when(mockEWAS.getSchemClass()).thenReturn(mockEWASSchemaClass);
		Mockito.when(mockRefSeq.getDBID()).thenReturn(1234L);
		// mismatch here on DBID should trigger a COSF prefix suggestion.
		Mockito.when(mockRefSeq2.getDBID()).thenReturn(1235L);
		Mockito.when(mockEWAS.getAttributeValue(ReactomeJavaConstants.referenceEntity)).thenReturn(mockRefSeq).thenReturn(mockRefSeq2);
		// need modified residues
		Mockito.when(mockFragReplaceSchemaClass.getName()).thenReturn(ReactomeJavaConstants.FragmentReplacedModification);
		Mockito.when(mockModResidue.getSchemClass()).thenReturn(mockFragReplaceSchemaClass);
		Mockito.when(mockModResidue.getDBID()).thenReturn(1234L);
		Mockito.when(mockFragInsertSchemaClass.getName()).thenReturn(ReactomeJavaConstants.FragmentInsertionModification);
		Mockito.when(mockModResidue2.getSchemClass()).thenReturn(mockFragInsertSchemaClass);
		Mockito.when(mockModResidue2.getDBID()).thenReturn(1234L);
		Mockito.when(mockResiduRefSeq.getDBID()).thenReturn(1234L);
		Mockito.when(mockResiduRefSeq2.getDBID()).thenReturn(1234L);
		Mockito.when(mockModResidue.getAttributeValue(ReactomeJavaConstants.referenceSequence)).thenReturn(mockResiduRefSeq);
		Mockito.when(mockModResidue2.getAttributeValue(ReactomeJavaConstants.referenceSequence)).thenReturn(mockResiduRefSeq2);
		Mockito.when(mockEWAS.getAttributeValuesList(ReactomeJavaConstants.hasModifiedResidue)).thenReturn(mockModResidues);

		mockModResidues.add(mockModResidue2);
		mockModResidues.add(mockModResidue);
		
		// set up an non-EWAS
		Mockito.when(mockNonEWASSchemaClass.getName()).thenReturn("NOT_AN_EWAS");
		Mockito.when(mockNonEWAS.getSchemClass()).thenReturn(mockNonEWASSchemaClass);
		
		// put EWAS and non-EWAS in list of EWASes
		EWASes.add(mockNonEWAS);
		EWASes.add(mockEWAS);
		
		// set up EWASes
		Mockito.when(mockCosmicObject2.getReferers(ReactomeJavaConstants.crossReference)).thenReturn(EWASes);
		Mockito.when(mockCosmicObject.getReferers(ReactomeJavaConstants.crossReference)).thenReturn(EWASes);
		
		// add both mocks to list
		cosmicObjects.add(mockCosmicObject);
		cosmicObjects.add(mockCosmicObject2);
		cosmicObjects.add(mockCosmicObject3);

		// test with list
		Map<String, COSMICIdentifierUpdater> result = COSMICUpdateUtil.determinePrefixes(cosmicObjects);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		
		for (String s : result.keySet())
		{
			System.out.println(s + "\t" + result.get(s).toString());
			if (s.contains("44444"))
			{
				// The 44444 COSMIC object should get COSF because of mismatch in one of the modified residues.
				assertTrue(result.get(s).getSuggestedPrefix().equals("COSF"));
			}
			if (s.contains("5678"))
			{
				// This should have a COSM prefix.
				assertTrue(result.get(s).getSuggestedPrefix().equals("COSM"));
			}
		}
		COSMICUpdateUtil.printIdentifierUpdateReport(result);
	}
}

