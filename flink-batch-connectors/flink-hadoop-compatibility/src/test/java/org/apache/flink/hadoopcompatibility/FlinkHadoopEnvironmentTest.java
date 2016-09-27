package org.apache.flink.hadoopcompatibility;

import org.apache.flink.api.java.utils.AbstractParameterToolTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.IOException;

public class FlinkHadoopEnvironmentTest extends AbstractParameterToolTest {

	@Test
	public void testParamsFromGenericOptionsParser() throws IOException {
		ParameterTool parameter = FlinkHadoopEnvironment.paramsFromGenericOptionsParser(new String[]{"-D", "input=myInput", "-DexpectedCount=15"});
		validate(parameter);
	}
}
