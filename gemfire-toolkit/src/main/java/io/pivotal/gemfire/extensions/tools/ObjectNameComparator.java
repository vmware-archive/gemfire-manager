package io.pivotal.gemfire.extensions.tools;

import java.util.Comparator;

import javax.management.ObjectName;

public class ObjectNameComparator implements Comparator<ObjectName>{

	@Override
	public int compare(ObjectName left, ObjectName right) {
		if (left == right) return 0; 
		return left.toString().compareTo(right.toString());
	}

}
