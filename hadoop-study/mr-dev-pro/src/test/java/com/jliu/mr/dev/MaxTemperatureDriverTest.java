package com.jliu.mr.dev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.Matchers.*;

public class MaxTemperatureDriverTest {
	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");  //use local file system
		conf.set("mapred.job.tracker", "local");  //use local tracker
		
		Path input = new Path("input/ncdc/micro/sample.txt");
		Path output = new Path("output");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {input.toString(), output.toString()});
		assertThat(exitCode, is(0));
		
	}
	
	private void checkOutput(Configuration conf, Path output) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
		assertThat(outputFiles.length, is(1));
		
		BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
		BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/expected.txt"));
		
		String expectedLine;
		while ((expectedLine = expected.readLine()) != null) {
			assertThat(actual.readLine(), is(expectedLine));
		}
		assertThat(actual.readLine(), nullValue());
		
		actual.close();
		expected.close();
	}
	
	public static class OutputLogFilter implements PathFilter {

//		@Override
		public boolean accept(Path path) {
			return !path.getName().startsWith("_");
		}
	}
	
	private BufferedReader asBufferedReader(InputStream in) {
		return new BufferedReader(new InputStreamReader(in));
	}
}
