package com.stackroute.datamunging.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.stackroute.datamunging.query.Field;
import com.stackroute.datamunging.query.Query;
import com.stackroute.datamunging.query.Restriction;

public class FilterHandler {

	final static Logger logger = Logger.getLogger(FilterHandler.class);

	// stream.filter( item -> item.startsWith("o") );
	/*
	 * persons .stream() .filter(p -> p.name.startsWith("P"))
	 * .collect(Collectors.toList());
	 */

	/*
	 * Map<Integer, List<Person>> personsByAge = persons .stream()
	 * .collect(Collectors.groupingBy(p -> p.age));
	 */
	/*
	 * Double averageAge = persons .stream() .collect(Collectors.averagingInt(p
	 * -> p.age));
	 */

	public static List<List<String>> filterRecords(List<List<String>> rows, Query query, List<String> header) {

		logger.debug("filterRecords");

		if (query.getRestrictions() != null) {

			rows = evaluateRestrictions(rows, query);
		}

		return rows;
	}

	private static List<List<String>> evaluateRestrictions(List<List<String>> rows, Query query) {
		List<Restriction> restrictions = query.getRestrictions();
		List<String> operators = query.getLogicalOperators();
		Predicate<List<String>> predicate = null;
		
		Predicate<List<String>> allPredicates = null;
		

	//	List<List<String>> originalRows = new ArrayList<>(rows);

		int operatorIndex = -1;

		for (Restriction restriction : restrictions) {

			switch (restriction.getCondition()) {
			case "=":

				predicate = p -> p.get(0).split(",")[restriction.getPropertyPosition()]
						.equals(restriction.getPropertyValue());
			   // predicate = predicate.negate();
				break;
			case "!=":

				predicate = p -> p.get(0).split(",")[restriction.getPropertyPosition()]
						.equals(restriction.getPropertyValue());
				//predicate = predicate.negate();
				predicate = predicate.negate();
				break;
				
			case ">":

				predicate = p ->  Integer.parseInt( p.get(0).split(",")[restriction.getPropertyPosition()])
						>(Integer.parseInt(restriction.getPropertyValue()));
				
				break;
				
			case "<":

				predicate = p ->  Integer.parseInt( p.get(0).split(",")[restriction.getPropertyPosition()])
						<(Integer.parseInt(restriction.getPropertyValue()));
				
				break;

			}
			

			if (operatorIndex == -1) {
				//rows.removeIf(predicate.negate());
				allPredicates = predicate;
				operatorIndex++;
				continue;
			}
			
			

			if (operators.get(operatorIndex).equalsIgnoreCase("and")) {
				//allPredicates.and(predicate);
				allPredicates.and(predicate);
			} else if (operators.get(operatorIndex).equalsIgnoreCase("or")) {
				allPredicates.or(predicate);
			}
			operatorIndex++;
		}
		 rows.removeIf(allPredicates.negate());
		//return rows.stream().distinct().collect(Collectors.toList());

		 return rows;

	}

	public static List<List<String>> filterFields(List<List<String>> result, Query query, List<String> header) {

		List<Field> selectedFields = query.getSelectedFields();
		Predicate<List<String>> predicate = null;
		Predicate<List<String>> allPredicates = null;

		for (List<String> row : result) {
			predicate = null;
			allPredicates = null;

			for (Field field : selectedFields) {
				predicate = p -> p.get(0).split(",")[field.getIndex()].equals(row.get(0).split(",")[field.getIndex()]);
				
			//	predicate = p -> p.get(0).split(",")[field.getIndex()] == field.getIndex());
				
				if (allPredicates == null) {
					allPredicates = predicate;
				} else {
					allPredicates.and(predicate);
				}

			}
			row.remove(allPredicates);

		}
		// result.remove(allPredicates.negate());

		/*
		 * for(Field field :selectedFields ) { predicate = p ->
		 * p.get(0).split(",")[field.getIndex()]
		 * .equals(result.get(0).get(field.getIndex()));
		 * result.stream().filter(predicate);
		 * 
		 * }
		 */

		/*
		 * List<List<String>> rows = new LinkedList<List<String>>(result);
		 * List<String> row = new LinkedList<String>();
		 */

		/*
		 * List<List<String>> rows = new
		 * java.util.ArrayList(Arrays.asList(result)); List<String> row;
		 * 
		 * Iterator rowIterator = rows.iterator(); Field field=null;
		 * while(rowIterator.hasNext()) { row = new
		 * java.util.ArrayList(Arrays.asList( (List<String>)
		 * rowIterator.next()));
		 * 
		 * Iterator fieldIterator = selectedFields.iterator();
		 * while(fieldIterator.hasNext()) { field = (Field)
		 * fieldIterator.next(); row.remove(field.getIndex()); }
		 * 
		 * }
		 */

		return result;

	}

}

/**
 * working... predicate = p ->
 * p.get(0).split(",")[restriction.getPropertyPosition()]
 * .equals(restriction.getPropertyValue().trim());
 * 
 */

// Predicate<List<String>> predicate = p-> p.get(0).split(",")[3].equals("Data
// Munging"); //just for test it is hard coded

/*
 * int position = restrictions.get(0).getPropertyPosition(); String
 * propertyValue = restrictions.get(0).getPropertyValue();
 * Predicate<List<String>> predicate1 = p->
 * p.get(0).split(",")[position].equals(propertyValue);
 */
// rows.removeIf(predicate.negate());
// return rows;

/*
 * return ((Collection<List<String>>) rows.stream()).
 * stream().filter(value->"Abbas".equals(value))
 * 
 * .collect(Collectors.toList());
 * 
 *//*
	 * return rows.stream() .filter(row->row.contains("Abbas"))
	 * 
	 * .collect(Collectors.toList());
	 */
