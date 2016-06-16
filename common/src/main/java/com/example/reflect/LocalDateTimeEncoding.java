package com.example.reflect;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 *
 */
public class LocalDateTimeEncoding extends CustomEncoding<LocalDateTime> {

    public LocalDateTimeEncoding() {
        schema = Schema.create(Schema.Type.STRING);
        schema.addProp("java-class", LocalDateTime.class.getName());
        schema.addProp("logicalType", "localDateTime");
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
        LocalDateTime dateTime = (LocalDateTime) datum;
        out.writeString(dateTime.toString());
    }

    @Override
    protected LocalDateTime read(Object reuse, Decoder in) throws IOException {
        return LocalDateTime.parse(in.readString());
    }
}
