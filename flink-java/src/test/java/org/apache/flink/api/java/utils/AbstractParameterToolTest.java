package org.apache.flink.api.java.utils;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public abstract class AbstractParameterToolTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	protected void validate(ParameterTool parameter) {
		ClosureCleaner.ensureSerializable(parameter);
		Assert.assertEquals("myInput", parameter.getRequired("input"));
		Assert.assertEquals("myDefaultValue", parameter.get("output", "myDefaultValue"));
		Assert.assertEquals(null, parameter.get("whatever"));
		Assert.assertEquals(15L, parameter.getLong("expectedCount", -1L));
		Assert.assertTrue(parameter.getBoolean("thisIsUseful", true));
		Assert.assertEquals(42, parameter.getByte("myDefaultByte", (byte) 42));
		Assert.assertEquals(42, parameter.getShort("myDefaultShort", (short) 42));

		Configuration config = parameter.getConfiguration();
		Assert.assertEquals(15L, config.getLong("expectedCount", -1L));

		Properties props = parameter.getProperties();
		Assert.assertEquals("myInput", props.getProperty("input"));
		props = null;

		// -------- test the default file creation ------------
		try {
			String pathToFile = tmp.newFile().getAbsolutePath();
			parameter.createPropertiesFile(pathToFile);
			Properties defaultProps = new Properties();
			try (FileInputStream fis = new FileInputStream(pathToFile)) {
				defaultProps.load(fis);
			}

			Assert.assertEquals("myDefaultValue", defaultProps.get("output"));
			Assert.assertEquals("-1", defaultProps.get("expectedCount"));
			Assert.assertTrue(defaultProps.containsKey("input"));

		} catch (IOException e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}
}
