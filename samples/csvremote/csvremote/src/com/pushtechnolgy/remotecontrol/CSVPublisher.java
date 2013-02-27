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
		int outseqnumindex;
		int topicseqnum;
		
		if (args.length < 3) {
			System.out.println("Usage: host:port filename topic");
			System.exit(0);
		}
		server=args[0];
		filename=args[1];
		topic=args[2];
		boolean filetopic=topic.startsWith("=");
		
		
		System.out.println("Host="+server+" Filename="+filename+" Topic="+topic);
	
		CSVReader reader = new CSVReader(new FileReader(filename));
		CSVDiffusion thePublisher = new CSVDiffusion(server);
		String [] nextLine;
		
		//read header
		String [] headerLine = reader.readNext();
		String [] newfields = new String[headerLine.length+1];
		System.arraycopy(headerLine, 0,newfields, 0, headerLine.length);
		newfields[headerLine.length]="PubSeqNum";
		
		if (filetopic) {
			topic=topic.substring(1,topic.length());
			for (int i=0; i<headerLine.length; i++) {
				if (headerLine[i]==topic) {
					topicindex=i;
				}
			}
		}
		
		try {
			thePublisher.CSVSetRecordType(newfields);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Thread.sleep(5000);
		thePublisher.CSVAddControlTopic("Control");
		if (!filetopic) {
			thePublisher.CSVAddTopic(topic);
		}
	
		while ((nextLine = reader.readNext()) != null) {
			System.arraycopy(nextLine, 0,newfields, 0, nextLine.length);
			newfields[nextLine.length]=Integer.toString(outseqnum);
			outseqnum++;

			if (filetopic) {
				if (!topicmap.containsKey(nextLine[topicindex])) {
					topicmap.put(nextLine[topicindex],1);
					topicseqnum=1;
					thePublisher.CSVAddTopic(nextLine[topicindex]);
				}
				else {
					topicseqnum=(Integer) topicmap.get(nextLine[topicindex]);
				}
				thePublisher.CSVPublish(nextLine[topicindex],newfields);
			}
			else {
				thePublisher.CSVPublish(topic,newfields);
			}
			// nextLine[] is an array of values from the line
			for (int i=0; i<nextLine.length; i++) {
				System.out.print(nextLine[i]+",");
			}
			System.out.println();
			Thread.sleep(500);
			
		}	

	}


}
