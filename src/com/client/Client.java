package com.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

import com.client.ClientMessage;


/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	//public static ChunkServer cs;
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private BufferedReader br;
	Socket s;
	
	/**
	 * Initialize the client
	 */
	public Client(int port, String hostname){
		//cs = new ChunkServer(port);
		//System.out.println("Starting client");
		s = null;
		try {
			s = new Socket(hostname, port);
			oos = new ObjectOutputStream(s.getOutputStream());
			ois = new ObjectInputStream(s.getInputStream());
			System.out.println("Connected!");
			
		} catch (IOException ioe) {
			System.out.println("Unable to connect to server with provided fields");
		}
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		ClientMessage message = new ClientMessage("initializeChunk", "", 0);
		//byte[] receivedBytes = sendBytes(message);
		//message = (ClientMessage)receiveObject(receivedBytes);
		try {
			oos.writeObject(message);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			message = (ClientMessage)ois.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return message.getChunkHandle();
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > ChunkServer.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		ClientMessage message = new ClientMessage("putChunk", ChunkHandle, offset);
		message.setPayload(payload);
		try {
			oos.writeObject(message);
			oos.flush();
			message = (ClientMessage)ois.readObject();
			if(message.getName().equals("true")) {
				return true;
			}
			else {
				return false;
			}
		} catch (IOException e) {
			System.out.println("I/O Exception in putChunk");
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			System.out.println("Class Not Found Exception");
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		ClientMessage message = new ClientMessage("getChunk", ChunkHandle, offset);
		message.setNumberOfBytes(NumberOfBytes);
		try {
			oos.writeObject(message);
			oos.flush();
			message = (ClientMessage)ois.readObject();
			
		} catch (IOException e) {
			System.out.println("I/O Exception in getChunk");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("Class Not Found Exception");
			e.printStackTrace();
		}
		return message.getPayload();
	}
	
	// https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
		public byte[] sendBytes (Object myObject) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = null;
			try {
			  out = new ObjectOutputStream(bos);   
			  out.writeObject(myObject);
			  out.flush();
			  byte[] yourBytes = bos.toByteArray();
			  return yourBytes;
			  // ...
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
			  try {
			    bos.close();
			  } catch (IOException ex) {
			    // ignore close exception
			  }
			}
			return null;
		}
	
	// https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
	public Object receiveObject(byte[] bos) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bos);
		ObjectInput in = null;
		try {
		  in = new ObjectInputStream(bis);
		  Object o = in.readObject(); 
		  return o;
		  //...
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		  try {
		    if (in != null) {
		      in.close();
		    }
		  } catch (IOException ex) {
		    // ignore close exception
		  }
		}
		return null;
	}
	
	public void closeSocket() {
		try {
			System.out.println("Trying to close");
			if (s != null) {
				s.close();
			}
		} catch (IOException ioe) {
			System.out.println("ioe: " + ioe.getMessage());
		}
	}

}