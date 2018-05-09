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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.projectriff.grpc.function.FunctionProtos.Message;
import io.projectriff.grpc.function.FunctionProtos.Message.Builder;
import io.projectriff.grpc.function.FunctionProtos.Message.HeaderValue;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;

import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.util.Assert;

/**
 * @author Dave Syer
 *
 */
public class MessageDeserializer implements Deserializer<Message> {

	private Gson gson = new Gson();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public Message deserialize(String topic, byte[] data) {
		Builder message = Message.newBuilder();
		int offset = 0;
		Assert.state(data[offset++] == -1, "Expected 0xff start marker");
		int headerCount = data[offset++];
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		for (int i=0; i<headerCount; i++) {
			int len = data[offset++];
			String name = new String(data, offset, len);
			offset += len;
			buffer.put(data, offset, 4);
			buffer.flip();
			int size = buffer.getInt();
			buffer.clear();
			offset += 4;
			List<String> values = gson.fromJson(new String(data, offset, size), new TypeToken<ArrayList<String>>() {}.getType());
			message.putHeaders(name, HeaderValue.newBuilder().addAllValues(values).build());
			offset += size;
		}
		message.setPayload(ByteString.copyFrom(data, offset, data.length - offset));
		return message.build();
	}

	@Override
	public void close() {
	}

}
