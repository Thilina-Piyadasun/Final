package org.abithana.prescription.impl.Redistricting;

import java.io.Serializable;
import java.util.*;

/**
 * Created by malakaganga on 1/2/17.
 */
public class Practice implements Serializable {
    public static void main(String[] args) {
        Practice practice = new Practice();
//        practice.test();
        //practice.test1();
        practice.test5();
    }
    public void test5(){

        ArrayList<CensusTract> censusTracts = new ArrayList<>();
        CensusTract ct1 = new CensusTract();
        ArrayList<Double> Lonpoint = new ArrayList<Double>();
        Lonpoint.add(-4.0);
        Lonpoint.add(-3.5);
        Lonpoint.add(-3.8);
        Lonpoint.add(-4.0);

        ArrayList<Double> Latpoint = new ArrayList<Double>();
        Latpoint.add(36.0);
        Latpoint.add(36.0);
        Latpoint.add(35.0);
        Latpoint.add(36.0);

        ct1.setPolygonLonPoints(Lonpoint);
        ct1.setPolygonLatPoints(Latpoint);

        CensusTract ct2 = new CensusTract();
        ArrayList<Double> Lonpoint2 = new ArrayList<Double>();
        Lonpoint2.add(-4.2);
        Lonpoint2.add(-3.2);
        Lonpoint2.add(-2.4);
        Lonpoint2.add(-5.0);
        Lonpoint2.add(-4.2);

        ArrayList<Double> Latpoint2 = new ArrayList<Double>();
        Latpoint2.add(36.0);
        Latpoint2.add(36.0);
        Latpoint2.add(39.06);
        Latpoint2.add(38.10);
        Latpoint2.add(36.0);

        ct2.setPolygonLonPoints(Lonpoint2);
        ct2.setPolygonLatPoints(Latpoint2);

        CensusTract ct3 = new CensusTract();
        ArrayList<Double> Lonpoint3 = new ArrayList<Double>();
        Lonpoint3.add(-3.9);
        Lonpoint3.add(-2.2);
        Lonpoint3.add(-4.2);
        Lonpoint3.add(-3.9);

        ArrayList<Double> Latpoint3 = new ArrayList<Double>();
        Latpoint3.add(37.04);
        Latpoint3.add(36.0);
        Latpoint3.add(36.0);
        Latpoint3.add(37.04);

        ct3.setPolygonLonPoints(Lonpoint3);
        ct3.setPolygonLatPoints(Latpoint3);

        censusTracts.add(ct1);
        censusTracts.add(ct3);
        censusTracts.add(ct2);


       /* CalculateIsoperimetricQuotient calculateIsoperimetricQuotient = new CalculateIsoperimetricQuotient();
        calculateIsoperimetricQuotient.isoperimetricQuotient(censusTracts);
*/

    }
    public void test4() {

        HashSet<Long> x = new HashSet<Long>();
        HashSet<Long> x1 = new HashSet<Long>();
        HashSet<Long> x2 = new HashSet<Long>();


        for (int i =0 ; i<= 5 ; i++) {
            x.add((long)i);
            x1.add((long)i);
        }

        Iterator it = x.iterator();
        while (it.hasNext()){
            System.out.println(" "+it.next());
        }

        System.out.println("kkkkkkkkkkkkkkkkkkkkkk");
        x.addAll(x1);
        Iterator it1 = x.iterator();
        while (it1.hasNext()){
            System.out.println(" "+it1.next());
        }


    }

    public void test3() {
        ArrayList<Double> list = new ArrayList<Double>();
        TreeSet<Double> tree = new TreeSet<Double>();
        for (int i = 5; i >= 0; i--) {
            list.add((double) i);
        }
        tree.addAll(list);

        for (int i = 0; i <= 5; i++) {
            System.out.println("from list =" + list.get(i) + " from tree =" + tree.last());
            tree.remove(tree.last());
        }
    }

    public void test2() {

        HashMap<Long, ArrayList<Long>> neighbourList = new HashMap<Long, ArrayList<Long>>();

        for (int i = 5; i >= 0; i--) {
            int id = i % 2;
            if (neighbourList.containsKey((long) id)) {
                ArrayList<Long> neighbours = neighbourList.get((long) id);
                neighbours.add((long) i);
            } else {
                ArrayList<Long> neighbors = new ArrayList<Long>();
                neighbors.add((long) i);
                neighbourList.put((long) id, neighbors);
            }
        }

        for (int i = 0; i < neighbourList.size(); i++) {
            System.out.println("\nId = " + i);
            ArrayList<Long> neighbours = neighbourList.get((long) i);
            for (int j = 0; j < neighbours.size(); j++) {
                System.out.print(" " + neighbours.get(j) + " ");
            }
        }
    }

    public void test1() {
        DistanceCalculator ds = new DistanceCalculator();
        System.out.println(" " + ds.distance(37.776016999, -122.444779999, 37.775802, -122.446471, "K"));

        ArrayList<CensusTract> al = new ArrayList<CensusTract>();
        for (CensusTract id : al) {
            System.out.println(" " + id.getPerimeter());
        }
        System.out.println(" size = " + al.size());

        ArrayList<Cluster> clusterCol = new ArrayList<Cluster>();
        ArrayList<Cluster> cluster2 = new ArrayList<Cluster>();

        for (int i = 1; i <= 6; i++) {

            Cluster cluster = new Cluster();
            cluster.setClusterId(i);
            cluster.setPopulation(i);
            double v = Math.random() * 100;
            cluster.setMinLongitude(v);
            v = Math.random() * 100;
            cluster.setMaxLongitude(v + 10);
            clusterCol.add(cluster);

        }

        Collections.sort(clusterCol, Cluster.longitudeComparatorMax);



        for (Cluster ct : clusterCol) {
            System.out.println(" Id =" + ct.getClusterId()+" "+ct.getMaxLongitude());
        }

        cluster2.addAll(clusterCol);

        Collections.sort(cluster2, Cluster.longitudeComparator);

        for (Cluster ct : clusterCol) {
            System.out.println(" Id =" + ct.getClusterId()+" "+ct.getMaxLongitude());
        }
        for (Cluster ct : cluster2) {
            System.out.println(" Id =" + ct.getClusterId()+" "+ct.getMaxLongitude());
        }



    }

    public void test() {

        Collection<Cluster> clusterCol = new ArrayList<Cluster>();
        HashMap<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();


        for (int i = 1; i <= 3; i++) {
            Cluster cluster = new Cluster();
            cluster.setClusterId(i);
            cluster.setPopulation(i);
            double v = Math.random() * 100;
            cluster.setMinLongitude(v);
            cluster.setMaxLongitude(v + 10);
            clusterCol.add(cluster);
            clusterMap.put((long) i, cluster);
        }
        for (int i = 4; i <= 6; i++) {
            Cluster cluster = new Cluster();
            cluster.setClusterId(i);
            cluster.setPopulation(i);
            double v = Math.random() * 100;
            cluster.setMinLongitude(v);
            cluster.setMaxLongitude(v + 10);


        }

        Iterator<Long> it = clusterMap.keySet().iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        Iterator<Long> it1 = clusterMap.keySet().iterator();
        while (it1.hasNext()) {
            long id = it1.next();
            if (id == 2) {
                it1.remove();
            }
        }

        Iterator<Long> it2 = clusterMap.keySet().iterator();
        System.out.println("After removal");
        while (it2.hasNext()) {
            System.out.println(it2.next());
        }

        /*for ( Cluster ct : clusterCol ) {
            System.out.println(ct.getMinLongitude());
        }

        Collections.sort((ArrayList)clusterCol, Cluster.longitudeComparator);

        System.out.println("After sorting in assending order");

        for ( Cluster ct : clusterCol ) {
            System.out.println(ct.getMinLongitude());
        }


        Cluster min = (Cluster) Collections.min((ArrayList) clusterCol, Cluster.longitudeComparator);

        System.out.println(min.getClusterId());

        Collections.sort((ArrayList)clusterCol, Cluster.longitudeComparatorMax);

        System.out.println("After sorting Max order");

        for ( Cluster ct : clusterCol ) {
            System.out.println(ct.getMaxLongitude());
        }

        System.out.println(" adada"+((ArrayList<Cluster>) clusterCol).get(clusterCol.size()-1).getMaxLongitude());

        Cluster cluster =  ((ArrayList<Cluster>) clusterCol).get(2);
        Cluster ct1 =  ((ArrayList<Cluster>) clusterCol).get(1);

        double minLong = (cluster.getMinLongitude() <= ct1.getMinLongitude() )? cluster.getMinLongitude() :
        ct1.getMinLongitude();
        System.out.println("MyMin = " + minLong);


        Cluster max1 = (Cluster) Collections.max((ArrayList) clusterCol, Cluster.longitudeComparatorMax);

        System.out.println(max1.getClusterId());*/


    }
}
