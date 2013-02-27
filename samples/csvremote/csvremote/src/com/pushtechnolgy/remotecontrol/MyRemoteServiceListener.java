package com.pushtechnolgy.remotecontrol;

import java.util.List;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.ClientDetails;
import com.pushtechnology.diffusion.api.message.TopicMessage;
import com.pushtechnology.diffusion.api.remote.RemoteRequest;
import com.pushtechnology.diffusion.api.remote.RemoteService;
import com.pushtechnology.diffusion.api.remote.RemoteServiceCloseReason;
import com.pushtechnology.diffusion.api.remote.RemoteServiceListener;
import com.pushtechnology.diffusion.api.topic.TopicSelector;

public class MyRemoteServiceListener implements RemoteServiceListener {
	private RemoteService theRemoteService;
	
	public void setRemoteService(RemoteService remoteService) {
		// TODO Auto-generated method stub
		theRemoteService=remoteService;
	}

	@Override
	public void clientConnected(ClientDetails clientDetails) {
		// TODO Auto-generated method stub
		System.out.println("Client connected " + clientDetails.getClientID());

	}

	@Override
	public void clientDetailsChanged(ClientDetails clientDetails) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientDisconnected(String clientId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientFetch(ClientDetails clientDetails, String topicName,
			List<String> headers) {
		
		System.out.println("Client Fetch " + clientDetails.getClientID() + topicName);
		try {
			theRemoteService.sendFetchReply(clientDetails.getClientID(),topicName,headers);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void clientSubscribe(ClientDetails clientDetails, String topicName) {
		// TODO Auto-generated method stub
		System.out.println("Client Subscribe " + clientDetails.getClientID() + topicName);
		try {
			theRemoteService.subscribeClient(clientDetails.getClientID(),topicName);
		} catch (APIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (topicName.matches("Control")) {
			
		}

	}

	@Override
	public void clientSubscribe(ClientDetails clientDetails,
			TopicSelector selector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientUnsubscribe(String clientId, String topicName,
			boolean hasSubscribers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closed(RemoteServiceCloseReason reason) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageFromClient(ClientDetails clientDetails,
			String topicName, TopicMessage message) {
		// TODO Auto-generated method stub
		System.out.println("Message from Client " + clientDetails.getClientID() + topicName + message);

	}

	@Override
	public void messageFromPublisher(TopicMessage message) {
		// TODO Auto-generated method stub
		System.out.println("Message from Publisher " + message);

	}

	@Override
	public void registerFailed(String errorMessage) {
		// TODO Auto-generated method stub
		System.out.println("Service Register Error "+errorMessage);

	}

	@Override
	public void registered() {
		// TODO Auto-generated method stub
		System.out.println("Service Registered");

	}

	@Override
	public void serviceRequest(RemoteRequest request) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void topicAddFailed(String topicName, String errorMessage) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void topicSubscribeFailed(String clientId, String topicName,
			String errorMessage) {
		// TODO Auto-generated method stub
		
	}

}
