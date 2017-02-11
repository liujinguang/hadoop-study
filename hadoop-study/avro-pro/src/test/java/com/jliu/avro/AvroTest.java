package com.jliu.avro;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class AvroTest {
	@Test
	public void testPairSpecific() throws IOException {
		StringPair datum = new StringPair();
		datum.setLeft("L");
		datum.setRight("R");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();

		DatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(StringPair.class);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringPair result = reader.read(null, decoder);

		assertEquals(result.getLeft().toString(), "L");
		assertEquals(result.getRight().toString(), "R");
	}

	@Test
	public void testPairGeneric() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		// create a record
		Schema schema = parser.parse(getClass().getClassLoader().getResourceAsStream("StringPair.avsc"));
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", new Utf8("L"));
		datum.put("right", new Utf8("R"));

		// output to stream
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();

		// read back the data
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		assertEquals(result.get("left").toString(), "L");
		assertEquals(result.get("right").toString(), "R");
	}

	@Test
	public void testDataFile() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("StringPair.avsc"));

		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", "L");
		datum.put("right", "R");

		File file = new File("data.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(datum);
		dataFileWriter.close();

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
		assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

		assertThat(dataFileReader.hasNext(), is(true));
		GenericRecord result = dataFileReader.next();
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
		assertThat(dataFileReader.hasNext(), is(false));

		dataFileReader.close();

		file.delete();
	}

	@Test
	public void testSchemaResolutionWIthDataFile() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("StringPair.avsc"));
		Schema newSchema = new Schema.Parser()
				.parse(getClass().getClassLoader().getResourceAsStream("NewStringPair.avsc"));

		File file = new File("data.avro");

		// create GenericRecord
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", "L");
		datum.put("right", "R");

		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(datum);
		dataFileWriter.close();

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(newSchema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);

		// schema is the actual (write) schema
		assertThat(schema, is(dataFileReader.getSchema()));

		assertThat(dataFileReader.hasNext(), is(true));
		GenericRecord result = dataFileReader.next();
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
		assertThat(result.get("description").toString(), is(""));
		assertThat(dataFileReader.hasNext(), is(false));

		dataFileReader.close();

		file.delete();
	}

	@Test
	public void testSchemaResolutionWithAliases() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("StringPair.avsc"));
		Schema newSchema = new Schema.Parser()
				.parse(getClass().getClassLoader().getResourceAsStream("AliasedStringPair.avsc"));
		
		// create GenericRecord
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", "L");
		datum.put("right", "R");		
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		writer.write(datum, encoder);
		encoder.flush();
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		
	    assertThat(result.get("first").toString(), is("L"));
	    assertThat(result.get("second").toString(), is("R"));

	    // old field names don't work
	    assertThat(result.get("left"), is((Object) null));
	    assertThat(result.get("right"), is((Object) null));		

	}
}
