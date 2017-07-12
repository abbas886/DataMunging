package com.stackroute.datamunging.query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import com.stackroute.datamunging.handler.FilterHandler;

public class Query {

	final static Logger logger = Logger.getLogger(Query.class);

	private String queryString;

	private boolean allFields;

	private List<Field> selectedFields;

	private String fileName;

	private List<Restriction> restrictions;

	private List<String> logicalOperators;

	private String groupBy;

	private String orderBy;

	private static final String SEPARATOR = ";";

	private static Path path;

	private List<List<String>> result;

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public boolean isAllFields() {
		return allFields;
	}

	public void setAllFields(boolean allFields) {
		this.allFields = allFields;
	}

	public List<Field> getSelectedFields() {
		return selectedFields;
	}

	public void setSelectedFields(List<Field> selectedFields) {
		this.selectedFields = selectedFields;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<Restriction> getRestrictions() {
		return restrictions;
	}

	public void setRestrictions(List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}

	/**
	 * select * from table where field='val'
	 * 
	 * @param queryString
	 * @return
	 */
	public  Query createQuery(String queryString) {
		logger.debug("createQuery with query string : " + queryString);
		String file = queryString.split("from")[1].split("\\s+")[1].trim();
		path = Paths.get(file);
		Query query = new Query();
		query.setQueryString(queryString);
		//addSelectedFields(query, queryString);
		addRescritions(query, queryString);
		addLogicalOperators(query, queryString);
		addGroupByField(query, queryString);
		addOrderByField(query, queryString);

		return query;

	}

	private static void addOrderByField(Query query, String queryString) {
		try {
			String orderBy = queryString.split("where")[1].split("order by")[1];
			query.setOrderBy(orderBy);
		} catch (Exception e) {
			logger.debug("No Order by  present");

		}

	}

	private static void addGroupByField(Query query, String queryString) {
		try {
			String groupBy = queryString.split("where")[1].split("group by")[1];
			query.setGroupBy(groupBy);
		} catch (Exception e) {
			logger.debug("No Group by  present");

		}

	}

	private static void addLogicalOperators(Query query, String queryString) {

		if (queryString.contains("where")) {
			String whereClauseQuery = queryString.split("where")[1].split("group by|order by")[0];

			String[] expressions = whereClauseQuery.split("and|or");

			List<String> logicalOperators = new ArrayList<String>();

			int size = expressions.length;
			int i = 0;
			for (String expression : expressions) {
				if (i++ < size - 1)
					logicalOperators.add(whereClauseQuery.split(expression.trim())[1].split("\\s+")[1]);

			}
			query.setLogicalOperators(logicalOperators);
		}

	}

	private static void addRescritions(Query query, String queryString) {
		// select * from table where field='val'
		// extract where conditions
		if (queryString.contains("where")) {

			String whereClauseQuery = queryString.split("where")[1];

			// select * from table where field1="val1" and field2!="val2" or
			// field3="val3
			/*
			 * Pattern pattern = Pattern.compile("[and]|[or]"); Matcher matcher
			 * = pattern.matcher(conditionString);
			 */
			// matcher.group();

			List<String> header = getHeader();
			String[] expressions = whereClauseQuery.split("and|or");

			String propertyName;
			String propertyValue;
			String condition;
			int propertyPosition;
			Restriction restriction;
			List<Restriction> restrictions = new ArrayList<Restriction>();
			if (whereClauseQuery != null) {
				for (String expression : expressions) {
					expression = expression.trim();
					condition = expression.split("\\s+")[1].trim();
					propertyName = expression.split(condition)[0].trim();
					propertyValue = expression.split(condition)[1].trim().replace("'", "");
					propertyPosition = header.indexOf(propertyName.trim());

					restriction = new Restriction(propertyPosition, propertyName, propertyValue, condition);
					restrictions.add(restriction);
				}
			}

			query.setRestrictions(restrictions);

		}
	}

	private static void addSelectedFields(Query query, String queryString) {
		String[] tokens = queryString.split("\\s+");

		if (tokens[1].equals("*")) {
			logger.debug(" will fetch all fields");

			query.setAllFields(true);
		} else { // select field1, field2, from ... will work???
			tokens = queryString.split("from")[0].split("\\s+");
			List<Field> fields = new ArrayList<Field>();
			Field field;
			for (int i = 1; i < tokens.length; i++) {
				field = new Field();
				field.setName(tokens[i]);
				field.setIndex(i - 1);
				field.setType("String");
				fields.add(field);
			}

			query.setSelectedFields(fields);
		}

	}

	/*
	 * public List<List<String>> executeQuery(Query query) { List<String> header
	 * = getHeader();
	 * 
	 * if (query.selectedFields != null) { result = filterSelectedFields(query);
	 * } else { result = getAllFields(query); }
	 * 
	 * FilterHandler.filterRecords(result, query, header);
	 * FilterHandler.filterFields(result, query, header); return result;
	 * 
	 * }
	 */
	private List<List<String>> getAllFields(Query query) {
		try (BufferedReader reader = Files.newBufferedReader(path)) {

			result = reader.lines().skip(1).map(line -> Arrays.asList(line.split(SEPARATOR)))
					.collect(Collectors.toList());

			return result;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private List<List<String>> filterSelectedFields(Query query) {
		String line;
		List<Field> selectedFields = query.getSelectedFields();
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			if ((line = reader.readLine()) != null) {
				line.replace(line.split(",")[0], "");
			}

			/*
			 * result = reader.lines().skip(1).map(line ->
			 * Arrays.asList(line.split(SEPARATOR)))
			 * .collect(Collectors.toList());
			 */

			return result;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<List<String>> executeQuery(Query query) {
		List<String> header = getHeader();

		try (BufferedReader reader = Files.newBufferedReader(path)) {
			result = reader.lines().skip(1).map(line -> Arrays.asList(line.split(SEPARATOR)))
					.collect(Collectors.toList());

			FilterHandler.filterRecords(result, query, header);
			//FilterHandler.filterFields(result, query, header);

			return result;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	private static List<String> header = new ArrayList<String>();

	private static List<String> getHeader() {
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			List<String> header = reader.lines().findFirst().map(line -> Arrays.asList(line.split(SEPARATOR))).get();
			return Arrays.asList(header.get(0).split("\\s*,\\s*"));

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}

// teams.stream().filter(p -> p.pointScored >= 60)