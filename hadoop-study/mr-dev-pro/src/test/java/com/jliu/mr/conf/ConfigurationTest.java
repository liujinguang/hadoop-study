package com.jliu.mr.conf;

import org.apache.hadoop.conf.Configuration;
import static org.hamcrest.core.Is.*;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigurationTest {
	@Test
	public void testConfigurationLoad() {
		Configuration conf = new Configuration();
		conf.addResource("configuration-1.xml");
		assertThat(conf.get("color"), is("yellow"));
		assertThat(conf.getInt("size", 0), is(10));
		
		//assign default value for undefined parameter
		assertThat(conf.get("breadth", "wide"), is("wide"));
	}
	
	@Test
	public void testConfigurationOverload() {
		Configuration conf = new Configuration();
		conf.addResource("configuration-1.xml");
		conf.addResource("configuration-2.xml");
		
		assertThat(conf.getInt("size", 0), is(12));
		assertThat(conf.get("weight"), is("heavy"));
		
		assertThat(conf.get("size-weight"), is("12, heavy"));
		
		//variable expansion with system properties
		System.setProperty("size", "14");
		assertThat(conf.get("size-weight"), is("14, heavy"));
		
	}
}
