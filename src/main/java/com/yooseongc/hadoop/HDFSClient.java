package com.yooseongc.hadoop;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient implements Closeable {

	private FileSystem hdfs = null;
	
	
	//=========================================
	// instantiation
	//=========================================
	
	private static HDFSClient instance;
	private HDFSClient() { }
	public static HDFSClient getInstance() {
		if ( instance == null ) {
			instance = new HDFSClient();
		} else {
			// do nothing
			System.out.println("already HDFSClient " + instance.hashCode() + " exists.");
		}
		
		return instance;   
	}
	
	public HDFSClient connect(String fs_url, String jt_url, String username) throws IOException {
		instance.setFileSystem(instance.getNewFileSystem(fs_url, jt_url, username));

		System.out.println(instance.getConfiguration().toString());
		System.out.println(instance.getFileSystem().toString());
		System.out.println("Home Path : " + instance.getFileSystem().getHomeDirectory());
		System.out.println("Work Path : " + instance.getFileSystem().getWorkingDirectory());
		return instance;
	}
	
	//=========================================
	// file access
	//=========================================
	
	public boolean existPath(String path_string) throws IllegalArgumentException, IOException {
		FileSystem hdfs = this.getFileSystem();
		return hdfs.exists(new Path(path_string));
	}
	
	public HDFSClient inspectPath(String path_string) throws IOException {
		FileSystem hdfs = this.getFileSystem();
		if (existPath(path_string)) {
			Path path = new Path(path_string);
			FileStatus[] fstats = hdfs.listStatus(path);
			System.out.println("CURRENT : " + path.toUri());
			for (FileStatus fstat : fstats) {
				System.out.println(fstat.getPath());
			}
		} else {
			// do nothing
		}
		
		return this;
	}
	
	public HDFSClient uploadFiles(String path_string, File[] files) throws IllegalArgumentException, IOException {
		FileSystem hdfs = this.getFileSystem();
		if (existPath(path_string)) {
			Path path = new Path(path_string);
			FSDataOutputStream output = hdfs.create(path);
			for (File file : files) {
				FileInputStream fi = new FileInputStream(file);
				BufferedInputStream bo = new BufferedInputStream(fi, 8192);
				while (true) {
					byte[] buffer = new byte[8192];
					if (bo.read(buffer) == -1) break;
					output.write(buffer);
				}
				bo.close();
				fi.close();
				output.flush();
			}
			System.out.println(hdfs.getContentSummary(path).toString());
		} else {
			// do nothing
		}
		
		return this;
	}
	
	
	//=========================================
	// get/set
	//=========================================
	
	public Configuration getConfiguration() {
		if (hdfs == null) return null;
		return hdfs.getConf();
	}
	
	public void setConfiguration(Configuration config) {
		if (hdfs == null) return;
		hdfs.setConf(config);
	}
	
	public FileSystem getFileSystem() {
		return hdfs;
	}
	
	public void setFileSystem(FileSystem hdfs) {
		this.hdfs = hdfs;
	}
	
	//========================================
	// configuration
	//========================================
	
	private Configuration getNewConfiguration(String fs_url, String jt_url, String username) {
		Configuration conf = new Configuration();
	    conf.set("fs.default.name", fs_url);
	    conf.set("mapred.job.tracker", jt_url);
	    conf.set("hadoop.job.ugi", username);
	    return conf;
	}
	
	private FileSystem getNewFileSystem(String fs_url, String jt_url, String username) throws IOException {
		return FileSystem.get(getNewConfiguration(fs_url, jt_url, username));
	}
	
	
	//========================================
	// overrides
	//========================================

	public void close() throws IOException {
		if (this.getFileSystem() != null) {
			System.out.println("Connection " + this.getFileSystem().toString() + " will be closed.");
			getFileSystem().close();
			this.setFileSystem(null);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s\n%s", this.getConfiguration().toString(), this.getFileSystem().toString());
		
	}

}
