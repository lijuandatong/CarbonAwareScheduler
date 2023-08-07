package uk.ac.gla.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test1 {
    public static void main(String[] args) {
        // 创建原始的 List
        List<Integer> originalList = new ArrayList<>();
        originalList.add(5);
        originalList.add(3);
        originalList.add(8);
        originalList.add(1);
        originalList.add(6);

        // 创建副本，并对副本进行排序
        List<Integer> sortedList = new ArrayList<>(originalList);
        Collections.sort(sortedList);

        // 打印原始的 List 和排序后的 List
        System.out.println("Original List: " + originalList);
        System.out.println("Sorted List: " + sortedList);
        System.out.println("Original List: " + originalList);

        List<Integer> top2 = sortedList.subList(0, 2);
        System.out.println("top2 List: " + top2);

        int index = originalList.indexOf(top2.get(0));
        System.out.println(originalList.get(index + 1));
    }
}
