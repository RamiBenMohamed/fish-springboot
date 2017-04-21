package me.ruslanys.components;

import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.ruslanys.Application;
import me.ruslanys.models.Event;
import me.ruslanys.models.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import java.io.IOException;
import java.util.Random;


@Component
@Slf4j
public class TaskProcessor {
	private static final Logger log = LoggerFactory.getLogger(TaskProcessor.class);
    private final JmsTemplate jmsTemplate;
    private final ObjectMapper mapper;

    private Random random = new Random();

    @Autowired
    public TaskProcessor(JmsTemplate jmsTemplate, ObjectMapper mapper) {
        this.jmsTemplate = jmsTemplate;
        this.mapper = mapper;
    }

    @Async
    @JmsListener(destination = Application.PROCESSOR_QUEUE)
    public void onMessage(String message) throws JMSException, JmsException, InterruptedException {
        log.debug("Got a message <{}>", message);
        try {
            Task task = mapper.readValue(message, Task.class);
            onMessage(task);
        } catch (IOException ex) {
            log.error("Encountered error while parsing message.", ex);
            throw new JMSException("Encountered error while parsing message.");
        }
    }

    @SneakyThrows
    private void onMessage(Task task) throws JmsException, JsonProcessingException, InterruptedException {
        log.info("Got a task: {}", task);

        // task started notification
        sendEvent(new Event(task.getId(), task.getType(), Event.Status.STARTED));

        // emulate task processing
        Thread.sleep(10_000);

        // task finished notification
        sendEvent(new Event(
                task.getId(),
                task.getType(),
                random.nextInt(2) == 1 ? Event.Status.SUCCESSFUL : Event.Status.FAILED
        ));
    }

    @SneakyThrows
    private void sendEvent(Event event) throws JmsException, JsonProcessingException {
      
     jmsTemplate.convertAndSend(Application.MANAGER_QUEUE, mapper.writeValueAsString(event));
    

    }

}
