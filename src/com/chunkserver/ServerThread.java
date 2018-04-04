package com.chunkserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
// Client Messge
import com.client.ClientMessage;

public class ServerThread extends Thread implements Serializable{
	
	public static final long serialVersionUID = 11;
	
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private ChunkServer cs;

	public ServerThread(Socket s, ChunkServer cs) {
		try {
			this.cs = cs;
			oos = new ObjectOutputStream(s.getOutputStream());
			ois = new ObjectInputStream(s.getInputStream());
			this.start();
		} catch (IOException ioe) {
			System.out.println("ioe: " + ioe.getMessage());
		}
	}
	
	public void sendMessage(ClientMessage message) {
		try {
			oos.writeObject(message);
			oos.flush();
		} catch (IOException ioe) {
			System.out.println("ioe: " + ioe.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				ClientMessage message = (ClientMessage)ois.readObject();
				//System.out.println("Received message: " + message.getMessage());
				if(message.getName().equals("initializeChunk")) {
					String chunk = cs.initializeChunk();
					message.setChunkHandle(chunk);
					sendMessage(message);
				}
				else if(message.getName().equals("putChunk")){
					boolean success = cs.putChunk(message.getChunkHandle(), message.getPayload(), message.getOffset());
					if(success) {
						message.setName("true");
					}
					else {
						message.setName("false");
					}
					sendMessage(message);
				}
				else if(message.getName().equals("getChunk")) {
					byte[] bytes = cs.getChunk(message.getChunkHandle(), message.getOffset(), message.getNumberOfBytes());
					message.setPayload(bytes);
					sendMessage(message);
				}
			}
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe in run: " + cnfe.getMessage());
		} catch (IOException ioe) {
			System.out.println("ioe in run: " + ioe.getMessage());
		}
	}
	
}