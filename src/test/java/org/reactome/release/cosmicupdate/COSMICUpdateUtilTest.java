package org.reactome.release.cosmicupdate;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

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
	}
	
	
	@Test
	public void testValidateAgainstFiles()
	{
		Map<String, COSMICIdentifierUpdater> updates = new HashMap<>();
		
		COSMICIdentifierUpdater updater1 = new COSMICIdentifierUpdater();
		COSMICIdentifierUpdater updater2 = new COSMICIdentifierUpdater();
		updates.put("1234", updater1);
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
}
