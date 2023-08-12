package com.atguigu.day02;

import java.util.ArrayList;
import java.util.HashMap;

// 有向无环图
public class Example2 {
    public static void main(String[] args) {
        // HashMap
        // {
        //    "A": ["B", "C"],
        //    "B": ["D", "E"],
        //    "C": ["D", "E"],
        // }
        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);

        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        topologicalSort(dag, "A", "A");
    }

    // topologicalSort(dag, "A", "A")
    // topologicalSort(dag, "B", "A => B")
    // topologicalSort(dag, "D", "A => B => D")
    public static void topologicalSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        // 递归的退出条件
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            // 遍历vertex顶点指向的所有顶点
            for (String v : dag.get(vertex)) {
                // 递归调用
                topologicalSort(dag, v, result + " => " + v);
            }
        }
    }
}
