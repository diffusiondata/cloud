/*
 * Copyright 2013 Push Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.pushtechnology.publisher;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.TimeoutException;
import com.pushtechnology.diffusion.api.data.TopicDataFactory;
import com.pushtechnology.diffusion.api.data.metadata.MDataType;
import com.pushtechnology.diffusion.api.data.single.SingleValueTopicData;
import com.pushtechnology.diffusion.api.message.TopicMessage;
import com.pushtechnology.diffusion.api.publisher.Client;
import com.pushtechnology.diffusion.api.publisher.Publisher;
import com.pushtechnology.diffusion.api.topic.Topic;

public class MyPublisher extends Publisher {
	   private Topic  TimeTopic= null;
	   private Topic  EchoTopic= null;
	   private static SingleValueTopicData TimeTopicData=null;

		static class ScheduledTask extends TimerTask {
			public void run() {
				Date date = new Date();

				try {
					TimeTopicData.updateAndPublish(date.toString());
				} catch (TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (APIException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

	   @Override
	    protected void initialLoad() throws APIException {
			Date date = new Date();
			System.out.println(date.toString());
			TimeTopicData = TopicDataFactory.newSingleValueData(MDataType.STRING);
			TimeTopicData.initialise(date.toString());
			TimeTopic=addTopic("TIME",TimeTopicData);
			EchoTopic=addTopic("ECHO");

			Timer time = new Timer();
			ScheduledTask tt = new ScheduledTask();
			time.schedule(tt,0,1000);

			logWarning("initialLoad called");

	   }
	   @Override	   
	   protected void publisherStarted() throws APIException {
		   logWarning("publisherStarted called");

	   }
	   
	   protected void subscription(Client client, Topic topic, boolean loaded) throws APIException {
	       logWarning("Subscription called...Topic = "+ topic.toString());

	   }
	   
	   protected void messageFromClient(TopicMessage message, Client client) {
	       logWarning("Message Received...Topic="+ message.getTopicName());
		   logWarning("Sending Message to "+client.getClientID());

	    	try {
				client.send(message);
			} catch (APIException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	   }
	   
	  
}
