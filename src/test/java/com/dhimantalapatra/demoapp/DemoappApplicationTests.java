package com.dhimantalapatra.demoapp;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class DemoappApplicationTests {

	@Autowired
	private MockMvc mockMvc;

	@Test
	void contextLoads() {
	}

	@Test
	void rendersSeparateConsumerPanels() throws Exception {
		mockMvc.perform(get("/"))
				.andExpect(status().isOk())
				.andExpect(content().string(containsString("consumer1-message-list")))
				.andExpect(content().string(containsString("consumer2-message-list")))
				.andExpect(content().string(containsString("hx-get=\"/messages/consumer2/fragment\"")));
	}

	@Test
	void rendersConsumer2MessageFragment() throws Exception {
		mockMvc.perform(get("/messages/consumer2/fragment"))
				.andExpect(status().isOk())
				.andExpect(content().string(containsString("consumer2-message-list")))
				.andExpect(content().string(containsString("hx-post=\"/messages/consumer2/replay\"")));
	}

	@Test
	void keepsConsumerMessagesSeparate() {
		MessageStore messageStore = new MessageStore();
		messageStore.add("consumer1", "one");
		messageStore.add("consumer2", "two");

		assertEquals(List.of("one"), messageStore.getMessages("consumer1"));
		assertEquals(List.of("two"), messageStore.getMessages("consumer2"));
	}
}
