package com.basic.rdma.util;

import org.apache.commons.cli.*;

public class CmdLineCommon {

	private static final String IP_KEY = "a";
	private String ip;

	private static final String PORT_KEY = "p";
	private int port;
	private static final int DEFAULT_PORT = 1919;

	private static final String SIZE_KEY = "s";
	private int size;
	private static final int DEFAULT_SIZE = 1024*1024;

	private static final String PATH_KEY = "f";
	private String path;

	private final String appName;

	private final Options options;

	public CmdLineCommon(String appName) {
		this.appName = appName;

		this.options = new Options();
		Option address = Option.builder(IP_KEY).required().desc("ip address").hasArg().required().build();
		Option path = Option.builder(PATH_KEY).desc("file path").hasArg().type(String.class).required().build();
		Option port = Option.builder(PORT_KEY).desc("port").hasArg().type(Number.class).build();
		Option size = Option.builder(SIZE_KEY).desc("size").hasArg().type(Number.class).build();

		options.addOption(address);
		options.addOption(port);
		options.addOption(size);
		options.addOption(path);
	}

	protected Options addOption(Option option) {
		return options.addOption(option);
	}

	public void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(appName, options);
	}

	protected void getOptionsValue(CommandLine line) throws ParseException {
		ip = line.getOptionValue(IP_KEY);
		if (line.hasOption(PORT_KEY)) {
			port = ((Number) line.getParsedOptionValue(PORT_KEY)).intValue();
		} else {
			port = DEFAULT_PORT;
		}
		if (line.hasOption(SIZE_KEY)) {
			size = ((Number) line.getParsedOptionValue(SIZE_KEY)).intValue();
		} else {
			size = DEFAULT_SIZE;
		}
		path = ((String) line.getParsedOptionValue(PATH_KEY)).toString();
	}

	public void parse(String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args);
		getOptionsValue(line);
	}


	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public int getSize() {
		return size;
	}

	public String getPath() {
		return path;
	}
}
