package com.chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer extends Thread implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public static long counter;
	
	private static ServerSocket serverSocket;
	private static int portNum = 1234;
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	
	// Fix
	public static int PayloadSize = Integer.SIZE/Byte.SIZE;
	public static int CommandSize = Integer.SIZE/Byte.SIZE;
	
	// Based on TA's design
	public static final int initOp = 100;
	public static final int putOp = 101;
	public static final int getOp = 102;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(filePath);
		File[] fs = dir.listFiles();
		
		if (fs == null || fs.length == 0) {
			counter = 0;
		} else {
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++) {
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			}
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
			if (cntrs.length == 0) {
				counter = 0;
			} else {
				Arrays.sort(cntrs);
				counter = cntrs[cntrs.length - 1];
			}
		}
		
		try {
			serverSocket = new ServerSocket(portNum);
		} catch (IOException e) {
			return;
		}
	}
	
	// Modified from https://stackoverflow.com/questions/10962736/opening-read-write-streams-multiple-times-from-a-socket
	public void run() {
		Socket s = null;
		while(true) {
			try {
				s = serverSocket.accept();			 				
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
				
				while (true) {
					int payloadLength = readInt(ois);
					// -1 indication to break, no need to do anything else
					if (payloadLength == -1)
						break;
					// Now, read "opcode"
					int opNum = readInt(ois);
					if (opNum == initOp) {
						runInit();
					} else if (opNum == putOp) {
						runPut(payloadLength);
					} else if (opNum == getOp) {
						runGet(payloadLength);
					}
				}     
			} catch (Exception e) {
				e.printStackTrace();
				break;
			} finally {
				try {
					if (s != null)
						s.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	// Handles initializeChunk
	private void runInit() {
		String chunk = initializeChunk();
		byte[] chunkHandlePacket = chunk.getBytes();
		try {
			oos.writeInt(chunkHandlePacket.length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} try {
			oos.write(chunkHandlePacket);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} try {
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// Handles putChunk
	private void runPut(int payloadLength) {
		int offset = readInt(ois);
		int payloadSize = readInt(ois);
		int chunkSize = (payloadLength)-(ChunkServer.PayloadSize)-(ChunkServer.CommandSize)-(payloadSize)-(2*4);
		
		byte[] payload = receivePayload(ois, payloadSize);
		byte[] chunkHandlePacket = receivePayload(ois, chunkSize);
		
		String chunk = new String(chunkHandlePacket);
		
		if (putChunk(chunk, payload, offset)) {
			try {
				oos.writeInt(1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				oos.writeInt(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	// Handles getChunk
	private void runGet(int payloadLength) {
		int offset = readInt(ois);
		int payloadsize = readInt(ois);
		int chunksize = payloadLength-ChunkServer.PayloadSize-ChunkServer.CommandSize-8;
		byte[] chunkHandlePacket = receivePayload(ois, chunksize);
		String chunk = new String(chunkHandlePacket);	
		
		byte[] output = getChunk(chunk, offset, payloadsize);
		if (output == null)
			try {
				oos.writeInt(ChunkServer.PayloadSize);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		else {
			try {
				oos.writeInt(ChunkServer.PayloadSize+output.length);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				oos.write(output);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private boolean isNumeric(String s) {  
		try {
			Long l = Long.valueOf(s);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;		
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 * 
	 * Not changed
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Read the chunk at the specific offset
	 * 
	 * Not changed
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	// Receives payload
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
	
	public static void main(String [] args) {
		Thread server = new ChunkServer();
		server.start();
	}	
}