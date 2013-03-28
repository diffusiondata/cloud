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
package com.pushtechnolgy.remotecontrol;

import java.util.*;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.connection.ConnectionFactory;
import com.pushtechnology.diffusion.api.connection.ServerDetails;

import com.pushtechnology.diffusion.api.data.TopicDataType;
import com.pushtechnology.diffusion.api.data.metadata.MDataType;
import com.pushtechnology.diffusion.api.data.metadata.MMessage;
import com.pushtechnology.diffusion.api.data.metadata.MRecord;
import com.pushtechnology.diffusion.api.data.metadata.MetadataFactory;
import com.pushtechnology.diffusion.api.message.DataMessage;
import com.pushtechnology.diffusion.api.message.MessageException;
import com.pushtechnology.diffusion.api.message.Record;
import com.pushtechnology.diffusion.api.message.TopicMessage;
import com.pushtechnology.diffusion.api.remote.RemoteService;
import com.pushtechnology.diffusion.api.remote.RemoteServiceFactory;
import com.pushtechnology.diffusion.api.remote.RemoteServiceListener;
import com.pushtechnology.diffusion.api.topic.Topic;
import com.pushtechnology.diffusion.api.remote.*;
import com.pushtechnology.diffusion.api.remote.topics.*;



public class CSVDiffusion {
	
	private	ServerDetails serverDetails = null;
	private RemoteService remoteService = null;
	private static final String CONTROL_TOPIC = "RemoteControl";
	

	private MMessage messageMetadata;
	private MRecord recordMetadata;
	private RecordTopicSpecification CSVTopicData=null;
	private RecordTopicSpecification CSVControlTopicData=null;
	private RemoteServiceListener theRemoteListener=null;
	private String [] topicFields;
	
	CSVDiffusion(String server) throws InterruptedException{
		
		String connectstring="dpt://"+server; 
		try {
			//set the connection info to provided
			serverDetails = ConnectionFactory.createServerDetails(connectstring);
			// connect to diffusion instance using remote control service provided
			// set our topic tree to CSVTopics
			remoteService = RemoteServiceFactory.createRemoteService(
							serverDetails, CONTROL_TOPIC, "CSVTopics", new MyRemoteServiceListener());			

			remoteService.register();
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		theRemoteListener=remoteService.getListener();
		((MyRemoteServiceListener) theRemoteListener).setRemoteService(remoteService);
	}
	
	// Create meta data for our messages - should be set of fields basaed on csv header line
	void CSVSetRecordType(String [] fields) throws APIException {
		
		topicFields = fields;
		messageMetadata = MetadataFactory.newMetadata("CSVMessage",TopicDataType.RECORD);
		recordMetadata=messageMetadata.addRecord("CSVRecord");
		for (int i=0; i<fields.length; i++) {
			recordMetadata.addField(fields[i]);
		}				
	}
	
	// Add topic to tree using above metadata as topic data
	void CSVAddTopic(String t) throws APIException {
		CSVTopicData=new RecordTopicSpecification(messageMetadata);
		try {
			remoteService.addTopic(t,CSVTopicData);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// Create meta data for our special topic to tell client field names 
	void CSVAddControlTopic(String t) throws APIException {
		CSVControlTopicData=new RecordTopicSpecification(messageMetadata);
		try {
			remoteService.addTopic(t,CSVControlTopicData);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		Record record = new Record(messageMetadata,topicFields);
		TopicMessage msg = null;
		try {
			msg = remoteService.createDeltaMessage("Control", 1024);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		msg.putRecords(record);
		remoteService.publish(msg);

	}

	// Publish Topic Data
	void CSVPublish(String topic, String[] t) throws APIException {
		
		//Get record based on metadata definition and store new values
		Record record = new Record(messageMetadata,t);
		
		//Get a message buffer
		TopicMessage msg = null;
		try {
			msg = remoteService.createDeltaMessage(topic, 1024);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		//Put record into message buffer and publish
		msg.putRecords(record);
		remoteService.publish(msg);
		
				
	}

}
