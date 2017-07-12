package com.stackroute.datamunging.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CSVFileReader{
	private static final String SEPARATOR = ";";

	private Path path;

	private String file;
	
	private String queryString;

	public CSVFileReader(String file) {
		this.file = file;
		path = Paths.get(file);

	}
	

	public String getQueryString() {
		return queryString;
	}


	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}


	public List<String> readHeader() {
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			return reader.lines().findFirst().map(line -> Arrays.asList(line.split(SEPARATOR))).get();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/*public List<List<String>> readRecords() {
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			return reader.lines().skip(1).map(line -> Arrays.asList(line.split(SEPARATOR)))
					.collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}*/

	
	
	

	
	
	
}













