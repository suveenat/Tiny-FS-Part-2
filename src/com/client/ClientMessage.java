package com.client;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class ClientMessage implements Serializable {
	public static final long serialVersionUID = 1;
	
	private String name;
	private String chunkHandle;
	private int offset;
	private int NumberOfBytes;
	private byte[] payload;
	public ClientMessage(String name, String chunkHandle, int offset) {
		this.name = name;
		this.chunkHandle = chunkHandle;
		this.offset = offset;
		NumberOfBytes = -1;
		payload = null;
	}
	
	public String getName() {
		return name;
	}
	
	public String getChunkHandle() {
		return chunkHandle;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public int getNumberOfBytes() {
		return NumberOfBytes;
	}
	
	public byte[] getPayload() {
		return payload;
	}
	
	public void setChunkHandle(String chunkHandle) {
		this.chunkHandle = chunkHandle;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public void setNumberOfBytes(int NumberOfBytes) {
		this.NumberOfBytes = NumberOfBytes;
	}
	
	public void setPayload(byte[] payload) {
		this.payload = payload;
	}
}