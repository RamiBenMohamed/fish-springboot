package me.ruslanys.services;

import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.ruslanys.Application;
import me.ruslanys.components.TaskProcessor;
import me.ruslanys.models.Event;
import me.ruslanys.models.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;


@Service
@Slf4j
public class TaskService {
	private static final Logger log = LoggerFactory.getLogger(TaskService.class);
    private final JmsTemplate jmsTemplate;
    private final ObjectMapper mapper;

    @Autowired
    public TaskService(JmsTemplate jmsTemplate, ObjectMapper mapper) {
        this.jmsTemplate = jmsTemplate;
        this.mapper = mapper;
    }

    @JmsListener(destination = Application.MANAGER_QUEUE)
    public void onMessage(String message) throws JMSException {
        log.debug("Got a message <{}>", message);
        try {
            Event event = mapper.readValue(message, Event.class);
            onMessage(event);
        } catch (Exception ex) {
            log.error("Encountered error while parsing message.",ex);
            throw new JMSException("Encountered error while parsing message.");
        }
    }

    private void onMessage(Event event) {
        log.info("Got an event: {}", event);
    }

    @Async
    @SneakyThrows
    public void start(Task task) throws JmsException, JsonProcessingException {
        String jsonInString = mapper.writeValueAsString(task);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl("")
                .withMessageBody(jsonInString);

       jmsTemplate.convertAndSend(sendMessageRequest);
    }

}
