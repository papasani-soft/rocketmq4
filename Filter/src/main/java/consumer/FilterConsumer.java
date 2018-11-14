package consumer;

import java.io.IOException;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class FilterConsumer {
	  public static void main(String[] args) throws InterruptedException, MQClientException, IOException {
		  DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");

		// only subsribe messages have property a, also a >=0 and a <= 3
		consumer.subscribe("TopicFilter7", MessageSelector.bySql("a between 0 and 3"));

		consumer.registerMessageListener(new MessageListenerConcurrently() {
		    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		    }
		});
		consumer.start();

	        System.out.printf("Consumer Started.%n");
	    }
}
