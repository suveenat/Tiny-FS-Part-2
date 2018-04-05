package com.chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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
	
	public static int PayloadSize = Integer.SIZE/Byte.SIZE;
	public static int CommandSize = Integer.SIZE/Byte.SIZE; //size of each command code
	
	// Random
	public static final int initCode = 100;
	public static final int putCode = 101;
	public static final int getCode = 102;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File[] files = (new File(filePath)).listFiles();
		if (files == null || files.length == 0) {
			counter = 0;
		}
		else { //in case of deleted files, we need to make sure a new file is the next sequential one
			Vector<Long> filenames = new Vector<Long>();
			for (File f: files) {
				if (isNumeric(f.getName())) //for hidden files such as .DS_Store on Mac 
					filenames.add(Long.valueOf(f.getName()));
			}
			
			if (filenames.size() == 0)
				counter = 0;
			else {
				Collections.sort(filenames);
				counter = filenames.lastElement();
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
					if (payloadLength == -1)
						// We don't need to do anything!
						break;
					int opNum = readInt(ois);
					if (opNum == initCode) {
						runInit();
					}
					else if (opNum == putCode) {
						runPut(payloadLength);
					}
					else if (opNum == getCode) {
						runGet(payloadLength);
					}
				}
			     
			} catch (Exception e) {
				e.printStackTrace();
				break;
			} finally {
				try {
					if (s!=null)
						s.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void runInit() {
		String chunk = initializeChunk();
		byte[] chunkHandlePayload = chunk.getBytes();
		try {
			oos.writeInt(chunkHandlePayload.length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			oos.write(chunkHandlePayload);
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
	}
	
	private void runPut(int payloadLength) {
		int offset = readInt(ois);
		int payloadsize = readInt(ois);
		byte[] payload = receivePayload(ois, payloadsize);
		int chunksize = payloadLength-ChunkServer.PayloadSize-ChunkServer.CommandSize-8-payloadsize;
		byte[] chunkHandlePayload = receivePayload(ois, chunksize);
		String chunk = new String(chunkHandlePayload);
		if (putChunk(chunk, payload, offset))
			try {
				oos.writeInt(1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else
			try {
				oos.writeInt(0);
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
	}

	
	private void runGet(int payloadLength) {
		int offset = readInt(ois);
		int payloadsize = readInt(ois);
		int chunksize = payloadLength-ChunkServer.PayloadSize-ChunkServer.CommandSize-8;
		byte[] chunkHandlePayload = receivePayload(ois, chunksize);
		String chunk = new String(chunkHandlePayload);	
		
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
	 * read the chunk at the specific offset
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