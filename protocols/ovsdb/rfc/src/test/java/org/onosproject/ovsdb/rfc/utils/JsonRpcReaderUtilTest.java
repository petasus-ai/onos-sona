/*
 * Copyright 2026-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.ovsdb.rfc.utils;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.onosproject.ovsdb.rfc.exception.UnsupportedException;
import org.onosproject.ovsdb.rfc.jsonrpc.JsonReadContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for JsonRpcReaderUtil.
 */
public class JsonRpcReaderUtilTest {

    private static final String MESSAGE = "{\"id\":\"1\",\"method\":\"echo\",\"params\":[]}";

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    @Test
    public void utf8IsAccepted() {
        assertThat(JsonRpcReaderUtil.isUtf8(bytes('{', '"', 'i', 'd')), is(true));
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0xEF, 0xBB, 0xBF, '{')), is(true));
    }

    @Test
    public void widerEncodingsAreRejected() {
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0xFE, 0xFF, 0, '{')), is(false));  // UTF-16 BE BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0xFF, 0xFE, '{', 0)), is(false));  // UTF-16 LE BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0, '{', 0, '"')), is(false));      // UTF-16 BE, no BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes('{', 0, '"', 0)), is(false));      // UTF-16 LE, no BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0, 0, 0xFE, 0xFF)), is(false));    // UTF-32 BE BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0xFF, 0xFE, 0, 0)), is(false));    // UTF-32 LE BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes(0, 0, 0, '{')), is(false));        // UTF-32 BE, no BOM
        assertThat(JsonRpcReaderUtil.isUtf8(bytes('{', 0, 0, 0)), is(false));        // UTF-32 LE, no BOM
    }

    @Test
    public void decodesUtf8JsonRpcMessage() throws Exception {
        ByteBuf in = Unpooled.copiedBuffer(MESSAGE, StandardCharsets.UTF_8);
        List<Object> out = new ArrayList<>();

        JsonRpcReaderUtil.readToJsonNode(in, out, new JsonReadContext());

        assertThat(out, hasSize(1));
        assertThat(((JsonNode) out.get(0)).get("method").asText(), is("echo"));
    }

    @Test(expected = UnsupportedException.class)
    public void rejectsUtf16JsonRpcMessage() throws Exception {
        ByteBuf in = Unpooled.copiedBuffer(MESSAGE, StandardCharsets.UTF_16LE);

        JsonRpcReaderUtil.readToJsonNode(in, out(), new JsonReadContext());
    }

    private static List<Object> out() {
        return new ArrayList<>();
    }
}
