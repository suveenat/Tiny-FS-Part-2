package com.client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {	
	private static Socket client;
	private static int portNum = 1234;
	private static String hostname = "localhost";

	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	
	/**
	 * Initialize the client
	 */
	public Client(){
		if (client!=null)
			return;
		
		try {
			client = new Socket(hostname, portNum);	          
			oos = new ObjectOutputStream(client.getOutputStream());
			ois = new ObjectInputStream(client.getInputStream());			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		try {
			oos.writeInt(ChunkServer.CommandSize);
			oos.writeInt(ChunkServer.initCode);
			oos.flush();
						
			int chunkSize = readInt(ois);
			byte[] chunkHandlePayload = receivePayload(ois, chunkSize);
			return (new String(chunkHandlePayload));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/*
	 * */
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > ChunkServer.ChunkSize){ // Check size
			// Make sure the right number of bits are read
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		try {
			byte[] chunkHandlePayload = ChunkHandle.getBytes();
			oos.writeInt(ChunkServer.PayloadSize+ChunkServer.CommandSize+4+4+payload.length+chunkHandlePayload.length);
			oos.writeInt(ChunkServer.putCode);
			oos.writeInt(offset);
			oos.writeInt(payload.length);
			oos.write(payload);
			oos.write(chunkHandlePayload);
			oos.flush();
			int output = readInt(ois);
			if (output==0)
				return false;
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		//return cs.getChunk(ChunkHandle, offset, NumberOfBytes);
		try {
			byte[] chunkHandlePayload = ChunkHandle.getBytes();
			oos.writeInt(ChunkServer.PayloadSize+ChunkServer.CommandSize+4+4+chunkHandlePayload.length);
			oos.writeInt(ChunkServer.getCode);
			oos.writeInt(offset);
			oos.writeInt(NumberOfBytes);
			oos.write(chunkHandlePayload);
			oos.flush();
			int chunkSize = readInt(ois) - ChunkServer.PayloadSize;
			return receivePayload(ois, chunkSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static byte[] receivePayload(ObjectInputStream in, int size) {
		byte[] tmp = new byte[size];
		byte[] buffer = new byte[size];
		int bytes = 0;
		
		while (bytes != size) {
			int counter=-1;
			try {
				counter = in.read(tmp, 0, (size-bytes)); //read in a payload carefully
				for (int j=0; j < counter; j++){
					buffer[bytes+j]=tmp[j];
				}
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			if (counter == -1) {
				return null;
			}
			else { 
				bytes += counter;	
			}
		} 
		return buffer;
	}

	// Modified from https://stackoverflow.com/questions/17455787/how-to-use-wrap-method-of-bytebuffer-in-java
		public static int readInt(ObjectInputStream oin) {
			byte[] buffer = receivePayload(oin, 4);
			ByteBuffer buff = null;
			if (buffer != null) {
				buff = ByteBuffer.wrap(buffer);
				return buff.getInt();
				//return ByteBuffer.wrap(buffer).getInt();
			}
			return -1;
		}
}