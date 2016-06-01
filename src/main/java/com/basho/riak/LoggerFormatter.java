package com.basho.riak;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LoggerFormatter extends Formatter {

	@Override
	public String format(LogRecord record) {
		StringBuilder sb = new StringBuilder();
		sb.append("[").append(record.getLevel()).append("] ");
		sb.append(record.getMessage());
		sb.append(System.lineSeparator());
		return sb.toString();
	}
	
}