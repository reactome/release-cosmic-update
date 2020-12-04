package org.reactome.release.cosmicupdate;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import org.gk.model.GKInstance;
import org.gk.model.InstanceDisplayNameGenerator;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.reactome.release.common.database.InstanceEditUtils;

public class COSMICIdentifierUpdaterTest
{

	private long creatorID = 112233445566L; 
	
	@Mock
	private MySQLAdaptor mockAdaptor;
	
	@Mock
	private GKInstance mockInstanceEdit;
	
	@Mock
	private GKInstance mockIdentifierObject;
	
	@Before
	public void set()
	{
		MockitoAnnotations.openMocks(this);
	}
	
	/**
	 * Tests attempting to update, but with no COSV identifier set, no update will happen.
	 */
	@Test
	public void testUpdateIdentifierNoUpdate()
	{
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		// We don't set a COSV identifier, triggering the "no update" execution path.
		updater.setValid(true);
		updater.setDbID(123465789L);
		
		try
		{
			try(MockedStatic<InstanceEditUtils> mockedStatic = Mockito.mockStatic(InstanceEditUtils.class))
			{
				Mockito.when(InstanceEditUtils.createDefaultIE(any(MySQLAdaptor.class), any(Long.class), any(Boolean.class), any(String.class))).thenReturn(mockInstanceEdit);
				updater.updateIdentfier(mockAdaptor, creatorID);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Tests an update.
	 */
	@Test
	public void testUpdateIdentifier()
	{
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		updater.setSuggestedPrefix("COSV");
		updater.setCosvIdentifier("COSV9393993");
		updater.setValid(true);
		updater.setDbID(123465789L);
		
		try
		{
			try(MockedStatic<InstanceEditUtils> mockedInstEdUtils = Mockito.mockStatic(InstanceEditUtils.class);
				MockedStatic<InstanceDisplayNameGenerator> mockedInstDisNameGen = Mockito.mockStatic(InstanceDisplayNameGenerator.class))
			{
				Mockito.when(InstanceEditUtils.createDefaultIE(any(MySQLAdaptor.class), any(Long.class), any(Boolean.class), any(String.class))).thenReturn(mockInstanceEdit);
				Mockito.when(InstanceDisplayNameGenerator.generateDisplayName(any(GKInstance.class))).thenReturn("TestDisplayName");
				Mockito.when(mockAdaptor.fetchInstance(any(Long.class))).thenReturn(mockIdentifierObject);
				updater.updateIdentfier(mockAdaptor, creatorID);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Tests update with a COSM suggested prefix and no COSV identifier.
	 */
	@Test
	public void testUpdateCOSMIdentifier()
	{
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		updater.setSuggestedPrefix("COSM");
		// testing COSM so don't set a COSV identiefier.
		updater.setValid(true);
		updater.setDbID(123465789L);
		
		try
		{
			try(MockedStatic<InstanceEditUtils> mockedInstEdUtils = Mockito.mockStatic(InstanceEditUtils.class);
				MockedStatic<InstanceDisplayNameGenerator> mockedInstDisNameGen = Mockito.mockStatic(InstanceDisplayNameGenerator.class))
			{
				Mockito.when(InstanceEditUtils.createDefaultIE(any(MySQLAdaptor.class), any(Long.class), any(Boolean.class), any(String.class))).thenReturn(mockInstanceEdit);
				Mockito.when(InstanceDisplayNameGenerator.generateDisplayName(any(GKInstance.class))).thenReturn("TestDisplayName");
				Mockito.when(mockIdentifierObject.getAttributeValue(ReactomeJavaConstants.identifier)).thenReturn("3333");
				Mockito.when(mockAdaptor.fetchInstance(any(Long.class))).thenReturn(mockIdentifierObject);
				
				updater.updateIdentfier(mockAdaptor, creatorID);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			fail();
		}
	}
}
