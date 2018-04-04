package com.chunkserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
//import java.util.Arrays;
import java.util.Vector;

import com.interfaces.ChunkServerInterface;

import com.chunkserver.ServerThread;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */



public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public static long counter;
	private static boolean connected;
	private transient BufferedReader br;
	private Vector<ServerThread> serverThreads;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(int port){
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
		
		ServerSocket ss = null;
		connected = false;
		serverThreads = new Vector<ServerThread>();
		br = new BufferedReader(new InputStreamReader(System.in));
		try {
			//System.out.println("Please enter the port to host the server.");
			//String line = br.readLine();
			//int port = Integer.parseInt(line);
			ss = new ServerSocket(port);
			connected = true;
			System.out.println("Server started!");
			while (true) {
				System.out.println("waiting for connection...");
				Socket s = ss.accept();
				System.out.println("connection from " + s.getInetAddress());
				ServerThread st = new ServerThread(s, this);
				serverThreads.add(st);
			}
		} catch (NumberFormatException nfe){
			System.out.print("Invalid port (nfe error).");
		} catch (IOException ioe) {
			System.out.print("Invalid port (i/o error).");
		} finally {
			if (ss != null) {
				try {
					ss.close();
				} catch (IOException ioe) {
					System.out.println("ioe closing ss: " + ioe.getMessage());
				}
			}
		}
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
	
	public static void main(String [] args) {
		while(!connected){
			new ChunkServer(5656);
		}
	}

}