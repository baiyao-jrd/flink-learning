package com.baiyao.flink.day02;

import java.util.ArrayList;
import java.util.HashMap;

/*
 * 有向无环图【hashmap】实现
 *
 * A,(B,C)
 * B,(D,E)
 * C,(D,E)
 *
 * */
public class Example2 {
    public static void main(String[] args) {
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

    private static void topologicalSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        //1. 递归退出条件
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            //2. 遍历vertex指向的所有顶点
            for (String s : dag.get(vertex)) {
                //3. 递归调用
                topologicalSort(dag,s,result + "=>" + s);
            }
        }
    }
}
