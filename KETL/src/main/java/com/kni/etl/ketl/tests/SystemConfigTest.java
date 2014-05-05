package com.kni.etl.ketl.tests;

import java.text.ParseException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.kni.etl.ketl.SystemConfig;
import com.kni.etl.ketl.SystemConfigCache;
import com.kni.etl.ketl.SystemConfigDirect;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.reader.JDBCSSAScanner;
import com.kni.etl.ketl.transformation.DimensionTransformation;
import com.kni.etl.ketl.writer.JDBCWriter;

public class SystemConfigTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetRequiredTags() throws ParseException, KETLThreadException {
		SystemConfig sc = new SystemConfigCache();

		sc.getRequiredTags(JDBCWriter.class, null);
		sc.getRequiredTags(JDBCWriter.class, "TERADATA");

	}

	@Test
	public void testGetRequiredTagsDetailed() throws ParseException, KETLThreadException {

		String[][] scd = resultsTag(SystemConfigDirect.getInstance());
		String[][] scc = resultsTag(SystemConfigCache.getInstance());

		for (int i = 0; i < scc.length; i++) {
			if (scc[i] != null)
				System.out.println(Arrays.hashCode(scc[i]) + " = " + Arrays.hashCode(scd[i]));
		}
		Assert.assertArrayEquals(scc, scd);

	}

	@Test
	public void testGetStepTemplate() throws ParseException, KETLThreadException {
		SystemConfig sc = new SystemConfigCache();
		sc.getStepTemplate(JDBCWriter.class, "TERADATA", "CREATETABLE", true);
		sc.getStepTemplate(JDBCWriter.class, "TERADATA", "CREATETABLE", false);
		sc.getStepTemplate(JDBCWriter.class, "TERADATA", "STATEMENTSEPERATOR", true);
		sc.getStepTemplate(JDBCWriter.class, "TERADATA", "STATEMENTSEPERATOR", false);
	}

	@Test
	public void testGetStepTemplateDetailed() throws ParseException, KETLThreadException {

		String[] scd = resultsTemplate(SystemConfigDirect.getInstance());
		String[] scc = resultsTemplate(SystemConfigCache.getInstance());
		for (int i = 0; i < scc.length; i++) {
			if (scc[i] != null)
				System.out.println(scc[i].hashCode() + " = " + scd[i].hashCode());
		}
		Assert.assertArrayEquals(scc, scd);

	}

	private String[][] resultsTag(SystemConfig scd) throws KETLThreadException {
		String[][] results = new String[3][];
		int i = 0;
		results[i++] = scd.getRequiredTags(JDBCReader.class, "ORACLE");
		results[i++] = scd.getRequiredTags(JDBCSSAScanner.class, "ORACLE");
		results[i++] = scd.getRequiredTags(DimensionTransformation.class, "ORACLE");
		return results;
	}

	private String[] resultsTemplate(SystemConfig scd) throws KETLThreadException {
		String[] results = new String[10];
		int i = 0;
		results[i++] = scd.getStepTemplate(JDBCReader.class, "ORACLE", "GETCOLUMNS", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "COMPLETENESS", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "COMPLETENESSCOLEXPRESSION", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "DOV", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "IBM", "COLUMNSTATS", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "COLUMNLOBSTATS", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "COLUMNQUERY", true);
		results[i++] = scd.getStepTemplate(JDBCSSAScanner.class, "ORACLE", "SAMPLE", true);
		results[i++] = scd.getStepTemplate(DimensionTransformation.class, "ORACLE", "MULTIINSERT", true);
		return results;
	}

	@Test
	public void testSystemConfig() throws ParseException, KETLThreadException {
		new SystemConfigCache();
	}

}
