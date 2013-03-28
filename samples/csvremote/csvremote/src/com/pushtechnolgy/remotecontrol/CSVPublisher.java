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
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import au.com.bytecode.opencsv.bean.HeaderColumnNameMappingStrategy;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

import java.beans.IntrospectionException;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.message.MessageException;
public class CSVPublisher {
		
	public static void main(String[] args) throws InterruptedException, IOException, APIException {
		String	server;
		String  filename;
		String  topic;
		int		topicindex=0;
		HashMap<String, Integer> topicmap = new HashMap<String, Integer>(); 
		int	outseqnum=1;
		
		if (args.length < 3) {
			System.out.println("Usage: host:port filename topic");
			System.exit(0);
		}
		server=args[0];
		filename=args[1];
		topic=args[2];
		boolean filetopic=topic.startsWith("=");
		
		
		System.out.println("Host="+server+" Filename="+filename+" Topic="+topic);
	
		//Diffusion Publisher
		CSVDiffusion thePublisher = new CSVDiffusion(server);
		Thread.sleep(5000);

		//CSV reader 
		CSVReader reader = new CSVReader(new FileReader(filename));
		

		
		String [] nextLine;	
		//read header
		String [] headerLine = reader.readNext();
		String [] newfields = new String[headerLine.length+1];
		
		// Add field for Publisher seq num at end
		System.arraycopy(headerLine, 0,newfields, 0, headerLine.length);
		newfields[headerLine.length]="PubSeqNum";
		
		//if topic is a column in file find it
		if (filetopic) {
			topic=topic.substring(1,topic.length());
			for (int i=0; i<headerLine.length; i++) {
				if (headerLine[i]==topic) {
					topicindex=i;
				}
			}
		}

		//Set record type to column names
		try {
			thePublisher.CSVSetRecordType(newfields);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		//Add control topic so client can build display from names
		thePublisher.CSVAddControlTopic("Control");
		
		//if single file topic add it
		if (!filetopic) {
			thePublisher.CSVAddTopic(topic);
		}
	
		while ((nextLine = reader.readNext()) != null) {
			System.arraycopy(nextLine, 0,newfields, 0, nextLine.length);
			newfields[nextLine.length]=Integer.toString(outseqnum);
			outseqnum++;

			if (filetopic) {
				//add topic if not already added
				if (!topicmap.containsKey(nextLine[topicindex])) {
					topicmap.put(nextLine[topicindex],1);
					thePublisher.CSVAddTopic(nextLine[topicindex]);
				}
				//publish
				thePublisher.CSVPublish(nextLine[topicindex],newfields);
			}
			else {
				//publish
				thePublisher.CSVPublish(topic,newfields);
			}
			// nextLine[] is an array of values from the line
			for (int i=0; i<nextLine.length; i++) {
				System.out.print(nextLine[i]+",");
			}
			System.out.println();
			//set to 2 lines per sec
			Thread.sleep(500);
			
		}	

	}


}
