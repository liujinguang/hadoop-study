package com.jliu.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
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
import org.apache.avro.util.Utf8;


public class AvroTest {
	
	public void avroParse() {
		Schema.Parser parser = new Schema.Parser();
		try {
			//create a record
			Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));
			GenericRecord datum = new GenericData.Record(schema);
			datum.put("left", new Utf8("L"));
			datum.put("right", new Utf8("R"));
			
			//output to stream
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			writer.write(datum, encoder);
			encoder.flush();
			out.close();
			
			//read back the data
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
			GenericRecord result = reader.read(null, decoder);
			System.out.println(result.get("left"));
			System.out.println(result.get("right"));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		AvroTest test = new AvroTest();
		test.avroParse();
	}
}
