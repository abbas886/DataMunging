package com.stackroute.datamunging.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunging.file.CSVFileReader;
import com.stackroute.datamunging.query.Query;

public class DataMungingTestCase {

	static CSVFileReader fileReader = null;

	static Query query = null;

	static String file = "D:/employee.csv";

	@BeforeClass
	public static void init() {
		try {
			fileReader = new CSVFileReader(file);
			query = new Query();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	//@Test
	public void readHeader() {

		List<String> header = fileReader.readHeader();
		assertNotNull("readHeader", header);
		displayHeader(header);
	}

	

   @Test
	public void getAllRecords() {

		query = query.createQuery("select * from D:/employee.csv");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv");
		displayRecords(records);
	}

   @Test
	public void restrictionEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department = 'Data Munging'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department = 'Data Munging'");
		displayRecords(records);
	}
	
	@Test
	public void restrictionNotEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department != 'Data Munging'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department != 'Data Munging'");
		displayRecords(records);
	}
	
	@Test
	public void restrictionEqualAndNotEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department = 'Data Munging' and Location != 'Bombay'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department = 'Data Munging'  and  Location != 'Bombay'");
		displayRecords(records);
	}
	
	@Test
	public void restrictionEqualAndEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department = 'Data Munging'  and  Location = 'Bombay'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department = 'Data Munging'  and  Location = 'Bombay'");
		displayRecords(records);
	}
	
	@Test
	public void restrictionEqualOrEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department = 'Data Munging'  or  Department = 'Hobes'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department = 'Data Munging'  or  Department = 'Hobes'");
		displayRecords(records);
	}
	
	//@Test
	public void restrictionNotEqualOrNotEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department != 'Data Munging' or Location != 'Bombay'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department != 'Data Munging'  or  Location != 'Bombay'");
		displayRecords(records);
	}
	
	//@Test
	public void restrictionNotEqualAndNotEqual() {

		query = query.createQuery("select * from D:/employee.csv where Department != 'Data Munging' and Location != 'Bombay'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\nselect * from D:/employee.csv where Department != 'Data Munging'  and  Location != 'Bombay'");
		displayRecords(records);
		
		
	}
	
	//@Test
	public void restrictionGreaterThan() {

		query = query.createQuery("select * from D:/employee.csv where Age > 70");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\n select * from D:/employee.csv where Age > 70");
		displayRecords(records);
	}
	
	@Test
	public void restrictionLessThanAndEqual() {

		query = query.createQuery("select * from D:/employee.csv where Age < 40 and Location != 'Bangalore'");
		List<List<String>> records = query.executeQuery(query);
		assertNotNull("filterData", records);
		System.out.println("\n select * from D:/employee.csv where Age < 40 and Location = 'Bangalore'");
		displayRecords(records);
	}
	
	//@Test
		public void getSelectedFields() {

			query = query.createQuery("select id, name, salary from D:/employee.csv");
			List<List<String>> records = query.executeQuery(query);
			assertNotNull("filterData", records);
			System.out.println("\nselect id, name, salary from D:/employee.csv");
			displayRecords(records);
		}




	private void displayRecords(List<List<String>> records) {
		records.forEach(System.out::println);

	}

	private void displayHeader(List<String> header) {
		System.out.println("\nHeader...");
		header.forEach(System.out::println);
	}

}
