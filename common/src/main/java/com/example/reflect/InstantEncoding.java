package com.example.reflect;

import java.io.IOException;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;

/**
 *
 */
public class InstantEncoding extends CustomEncoding<Instant> {

    public InstantEncoding() {
        schema = Schema.create(Schema.Type.STRING);
        schema.addProp("java-class", Instant.class.getName());
        schema.addProp("logicalType", "instant");
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
        Instant instant = (Instant) datum;
        out.writeString(instant.toString());
    }

    @Override
    protected Instant read(Object reuse, Decoder in) throws IOException {
        return Instant.parse(in.readString());
    }
}
