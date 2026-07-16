package org.reactome.release.cosmicupdate;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import org.gk.model.ReactomeJavaConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.reactome.curation.model.SimpleInstance;

public class COSMICIdentifierUpdaterTest {
	@Mock
	private SimpleInstance mockIdentifierObject;
	
	@Before
	public void set() {
		MockitoAnnotations.openMocks(this);
	}
	
	/**
	 * Tests attempting to update, but with no COSV identifier set, no update will happen.
	 */
	@Test
	public void testUpdateIdentifierNoUpdate() {
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		// We don't set a COSV identifier, triggering the "no update" execution path.
		updater.setValid(true);
		updater.setCosmicDatabaseIdentifierInstance(mockIdentifierObject);

		try {
			updater.updateIdentifier();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Tests an update.
	 */
	@Test
	public void testUpdateIdentifier() {
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		updater.setSuggestedPrefix("COSV");
		updater.setCosvIdentifier("COSV9393993");
		updater.setValid(true);
		updater.setCosmicDatabaseIdentifierInstance(mockIdentifierObject);

		try {
			Mockito.when(updater.generateDisplayName(any(SimpleInstance.class))).thenReturn("TestDisplayName");
			//Mockito.when(mockAdaptor.fetchInstance(any(Long.class))).thenReturn(mockIdentifierObject);
			updater.updateIdentifier();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Tests update with a COSM suggested prefix and no COSV identifier.
	 */
	@Test
	public void testUpdateCOSMIdentifier() {
		COSMICIdentifierUpdater updater = new COSMICIdentifierUpdater();
		updater.setIdentifier("123456");
		updater.setSuggestedPrefix("COSM");
		// testing COSM so don't set a COSV identifier.
		updater.setValid(true);
		updater.setCosmicDatabaseIdentifierInstance(mockIdentifierObject);

		try {
			Mockito.when(updater.generateDisplayName(any(SimpleInstance.class))).thenReturn("TestDisplayName");
			Mockito.when(mockIdentifierObject.getAttribute(ReactomeJavaConstants.identifier)).thenReturn("3333");
			//Mockito.when(mockAdaptor.fetchInstance(any(Long.class))).thenReturn(mockIdentifierObject);

			updater.updateIdentifier();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
