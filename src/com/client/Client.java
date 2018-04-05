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
	private static Socket cs;
	private static int portNum = 1234;
	private static String host = "localhost";

	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	
	/**
	 * Initialize the client
	 */
	public Client() {
		if (cs != null)
			return;
		try {
			cs = new Socket(host, portNum);	          
			oos = new ObjectOutputStream(cs.getOutputStream());
			ois = new ObjectInputStream(cs.getInputStream());			
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
			oos.writeInt(ChunkServer.initOp);
			oos.flush();
						
			int chunkSize = readInt(ois);
			byte[] chunkHandlePacket = parsePayload(ois, chunkSize);
			return (new String(chunkHandlePacket));
	
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
		// Same as Part 1
		if(offset + payload.length > ChunkServer.ChunkSize){ // Check size
			// Make sure the right number of bits are read
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		// New for Part 2
		try {
			byte[] chunkHandlePacket = ChunkHandle.getBytes();
			// Part by part, send over
			oos.writeInt((ChunkServer.PayloadSize)+(ChunkServer.CommandSize)+(payload.length)+(chunkHandlePacket.length)+(2*4));
			oos.writeInt(ChunkServer.putOp);
			oos.writeInt(offset);
			oos.writeInt(payload.length);
			oos.write(payload);
			oos.write(chunkHandlePacket);
			// Then flush out
			oos.flush();
			int output = readInt(ois);
			if (output == 0) {
				return false;
			} else {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		// Part 1
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		// New for Part 2
		} try {
			byte[] chunkHandlePacket = ChunkHandle.getBytes();
			// Part by part, send over
			oos.writeInt((ChunkServer.PayloadSize)+(ChunkServer.CommandSize)+(chunkHandlePacket.length)+(2*4));
			oos.writeInt(ChunkServer.getOp);
			oos.writeInt(offset);
			oos.writeInt(NumberOfBytes);
			oos.write(chunkHandlePacket);
			
			// Flush!
			oos.flush();
			int chunkSize = readInt(ois) - ChunkServer.PayloadSize;
			return parsePayload(ois, chunkSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/* Parses payload -- modified from https://stackoverflow.com/questions/2183240/java-integer-to-byte-array
	 * Same implementation as in ChunkServer
	 */
	public static byte[] parsePayload(ObjectInputStream ois, int size) {
		byte[] tempBuff = new byte[size];
		byte[] buffer = new byte[size];
		int numBytes = 0;
		
		while (numBytes != size) {
			int counter = -1;
			try {
				counter = ois.read(tempBuff, 0, (size-numBytes));
				for (int j=0; j < counter; j++){
					buffer[numBytes+j]=tempBuff[j];
				}
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			if (counter == -1) {
				return null;
			} else { 
				numBytes += counter;	
			}
		} 
		return buffer;
	}

	/* Modified from https://stackoverflow.com/questions/17455787/how-to-use-wrap-method-of-bytebuffer-in-java
	 * Same implementation as in ChunkServer
	 */
	public static int readInt(ObjectInputStream ois) {
		// Previous function, use 4 for size bc int
		byte[] buffer = parsePayload(ois, 4);
		ByteBuffer buff = null;
		if (buffer != null) {
			buff = ByteBuffer.wrap(buffer);
			return buff.getInt();
		}
		return -1;
	}
}