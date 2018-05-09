/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import io.projectriff.grpc.function.FunctionProtos.Message;

import com.google.gson.Gson;

import org.apache.kafka.common.serialization.Serializer;

/**
 * @author Dave Syer
 *
 */
public class MessageSerializer implements Serializer<Message>  {

	private Gson gson = new Gson();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, Message data) {
		int len = 2;
		Map<String, byte[]> headers = new LinkedHashMap<>();
		for (String name : data.getHeadersMap().keySet()) {
			len += 1 + name.length();
			byte[] value = gson.toJson(data.getHeadersMap().get(name).getValuesList()).getBytes();
			headers.put(name, value);
			len += 4 + value.length;
		}
		len += data.getPayload().size();
		byte[] bytes = new byte[len];
		bytes[0] = -1;
		bytes[1] = (byte) headers.size();
		int offset = 2;
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		for (String name : headers.keySet()) {
			len = name.length();
			bytes[offset++] = (byte) len;
			System.arraycopy(name.getBytes(), 0, bytes, offset, len);
			offset += len;
			byte[] value = headers.get(name); 
			buffer.putInt(value.length);
			buffer.flip();
			System.arraycopy(buffer.array(), 0, bytes, offset, Integer.BYTES);
			buffer.clear();
			offset += Integer.BYTES;
			len = value.length;
			System.arraycopy(value, 0, bytes, offset, len);
			offset += len;
		}
		System.arraycopy(data.getPayload().toByteArray(), 0, bytes, offset, data.getPayload().size());
		return bytes;
	}

	@Override
	public void close() {
	}

}
