package common;

import java.util.ArrayList;
import java.util.List;

public class Utils {
	public static <T> List<T>[] getNeighbors(T[] arr) {
		List<T>[] neighbors = new ArrayList[arr.length];
		for (int i = 0; i < arr.length; i++) {
			List<T> list = new ArrayList<>();
			for (int j = i + 1; j < arr.length; j++) {
				if (arr[j].equals(arr[i])) {
					break;
				}
				list.add(arr[j]);
			}
			neighbors[i] = list;
		}
		return neighbors;
	}
}
