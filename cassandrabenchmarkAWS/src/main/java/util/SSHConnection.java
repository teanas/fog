package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * Creates ssh connection and executes commands
 */
public class SSHConnection {
	
	private String hostName;
    private String userName;
    private String keyfile;
    private Session session;
    private String internalName;
    private boolean connectionSuccessful = false;
    
    private static Logger logger = LoggerFactory.getLogger(SSHConnection.class);
    
	public SSHConnection(String hostName, String internalName, String userName, String keyfile) {
		this.hostName = hostName;
		this.internalName = internalName;
		this.userName = userName;
		this.keyfile = keyfile;

		JSch jsch=new JSch();
		try {
			jsch.addIdentity(keyfile);
			this.session=jsch.getSession(userName, hostName, 22);
			java.util.Properties config = new java.util.Properties(); 
			config.put("StrictHostKeyChecking", "no");
			this.session.setConfig(config);
			
			logger.info("Connecting to " + hostName + " via SSH");
			
			boolean connected = false;
			while(!connected) {
				try {
					session.connect();
					connected = true;
					connectionSuccessful = true;
					logger.info("Connected to " + hostName);
				}catch (com.jcraft.jsch.JSchException e) {
					if(e.getCause() != null && e.getCause().getMessage().equals("Connection refused: connect")) {
						logger.error("Connection to " + hostName + "refused, SSH client isn't started yet");
					}else {
						logger.error("Unable to connect to " + hostName + ":" + e.getMessage(), e);
					}
				}catch (Exception e) {
					logger.error("Unable to connect to " + hostName + ":" + e.getMessage(), e);
				}
				Thread.sleep(2000);
			}
		} catch (Exception e1) {
			logger.error("Unable to connect to " + hostName + ":" + e1.getMessage(), e1);
		}
	}
	
	/**
	 * Updates internal Name of instance
	 * @param name - new name
	 */
	public void setInternalName(String name) {
		this.internalName = name;
	}
	
	public boolean isConnected() {
		return connectionSuccessful;
	}
	
	private String getOutput(Channel channel, boolean withReturn, boolean toConsole){
		String result = "";
		try {
			InputStream input = channel.getInputStream();
			//start reading the input from the executed commands on the shell
			int len = toConsole? 5 : 1024 * 8; // 1024
			byte[] tmp = new byte[len];
			while (true) {
			    while (input.available() > 0) {
			        int i = input.read(tmp, 0, len);
			        if (i < 0) break;
			        String c = new String(tmp, 0, i);
			        if(withReturn) result += c; 
			        if(toConsole) logger.info(c);
			    }
			    if (channel.isClosed()){
			    	if(channel.getExitStatus()!=0) {
			    		logger.warn("exit status was " + channel.getExitStatus());
			    	}
			        break;
			    }
			    try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e) {
					logger.error("Error while reading shell output",e);
				}
			}
		}catch (IOException e) {
			logger.error("Error while reading shell output",e);
		}
		return result;
	}
	
	private String runCommand(String command, boolean withReturn, boolean toConsole) {
		logger.info("Executing on " + internalName + ": " + command);
		return runCommandWithOutText(command, withReturn, toConsole);
	}

	private String runCommandWithOutText(String command, boolean withReturn, boolean toConsole) {
		String output = hostName + ": Connection Error";
		try {
			ChannelExec channel;
			channel = (ChannelExec) this.session.openChannel("exec");
			channel.setInputStream(null);
			channel.setErrStream(System.err);
			channel.setCommand(command);
			channel.connect();
			output = getOutput(channel, withReturn, toConsole);
			channel.disconnect(); 
		} catch (JSchException e) {
			logger.error("Error while executing command " + command, e);
		}
        return output;
	}
	
	public void runCommandDoNothing(String command) {
		runCommand(command, false, false);
	}
	
	public void runCommandsDoNothing(List<String> command) {
		runCommand(combineCommands(command), false, false);
	}
	
	public String runCommandFromConsole(String command) {
		return runCommandWithOutText(command, false, true);
	}
	
	
	public void runCommandWithoutOutput(String command) {
		runCommandWithOutText(command, false, false);
	}
	
	public String runCommandWithoutOutputreturnString(String command) {
		return runCommandWithOutText(command, true, false);
	}
	
	public String runCommand(String command) {
		return runCommand(command, true, false);
	}
		
	public String runCommandToConsole(String command) {
		return runCommand(command, false, true);
	}
	
	public String runCommand(List<String> command) {
		return runCommand(combineCommands(command), true, false);
	}
	
	public void runCommandToConsole(List<String> command) {
		runCommand(combineCommands(command), false, true);
	}
	
	public String runCommandToConsoleAndString(String command) {
		return runCommand(command, true, true);
	}
	
	public String runCommandToConsoleAndString(List<String> command) {
		return runCommand(combineCommands(command), true, true);
	}
	
	private String combineCommands(List<String> command) {
		return String.join(" && ", command);
	}
	
	/**
	 * copies remote file to local directory
	 * @param remote path and file
	 * @param local path
	 */
	public void copyFile(String remote, String local){
		Channel channel;
		try {
			channel = this.session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			sftpChannel.get(remote, local);
			sftpChannel.exit();
		} catch (JSchException e) {
			logger.error("Unable to copy file :" , e);
		} catch (SftpException e) {
			logger.error("Unable to copy file :" , e);
		}
	}
	
	public void disconnect() {
		if (session != null) {
			session.disconnect();
			logger.info("Disconnected from " + hostName);
		}
		connectionSuccessful = false;
	}

}
