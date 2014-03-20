/**
 * 
 */
package core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author zangetsu
 *
 */
public class Solap4pyTest {

	Solap4py s;
	
	@Before
	public void avantTest(){
		s = new Solap4py();
	}
	
	
	@Test
	public void testSelect(){
		assertEquals(true,true);
	}
	@Test
	public void testGetMetadata(){
		assertEquals(true,true);
	}

}
