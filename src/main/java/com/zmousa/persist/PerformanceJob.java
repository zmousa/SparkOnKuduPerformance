package com.zmousa.persist;

import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PerformanceJob implements Serializable {

    private static final String KUDU_MASTER = "cloudera-dev-mn0.germanycentral.cloudapp.microsoftazure.de";
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KuduOnSpark")
                .setMaster("local[*]");
        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        String masters = String.format("%s:7051", KUDU_MASTER);
        String kuduTableName = "impala::default.sfmta";

        // first index key (for primary key)
        int startIndex = Integer.parseInt(args[0]);
        // total number of generated rows
        int total = Integer.parseInt(args[1]);

        Map<String, String> kuduOptions = new HashMap();
        kuduOptions.put("kudu.table", kuduTableName);
        kuduOptions.put("kudu.master", masters);
        KuduContext kuduContext = new KuduContext(masters, sparkSession.sparkContext());

        createDummyData(sparkSession, kuduContext, kuduTableName, startIndex, total);
        scanAndUpdateData(sparkSession, kuduContext, kuduOptions, kuduTableName);
    }

    private static void createDummyData(SparkSession sparkSession,
                                        KuduContext kuduContext, final String table, int startIndex, int total) {
        System.out.println("############### Start Insertion ############### ");
        System.out.println(dateFormat.format(new Date()));
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        List<Sfmta> rows = new ArrayList<>();
        for (int i =startIndex; i < startIndex + total; i++){
            rows.add(new Sfmta(new Long(i),
                    ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE),
                    Float.MIN_VALUE + ThreadLocalRandom.current().nextFloat() * (Float.MAX_VALUE - Float.MIN_VALUE),
                    Float.MIN_VALUE + ThreadLocalRandom.current().nextFloat() * (Float.MAX_VALUE - Float.MIN_VALUE),
                    4,
                    Float.MIN_VALUE + ThreadLocalRandom.current().nextFloat() * (Float.MAX_VALUE - Float.MIN_VALUE)));
            if (rows.size() == 10000){
                JavaRDD<Sfmta> rdd = jsc.parallelize(rows);
                Dataset rowsDf = sparkSession.sqlContext().createDataFrame(rdd, Sfmta.class);
                kuduContext.insertRows(rowsDf, table);
                rows.clear();
            }
        }
        System.out.println("############### Done Insertion ############### ");
        System.out.println(dateFormat.format(new Date()));
    }

    private static void scanAndUpdateData(SparkSession sparkSession, KuduContext kuduContext, Map<String, String> kuduOptions, final String table){
        String registeredTableName = "sfmta";
        System.out.println("############### Start Loading ############### ");
        System.out.println(dateFormat.format(new Date()));
        sparkSession.sqlContext().read().format("org.apache.kudu.spark.kudu").options(kuduOptions).load()
                .registerTempTable(registeredTableName);
        Dataset kuduDf = sparkSession.sqlContext().sql("SELECT * FROM " + registeredTableName + " WHERE speed = 4");
        kuduDf.show();
        System.out.println("############### Done Loading ############### ");
        System.out.println(dateFormat.format(new Date()));

        StructType schema = new StructType(new StructField[]{
                new StructField("report_time", DataTypes.LongType, false, Metadata.empty()),
                new StructField("vehicle_tag", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("longitude", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("latitude", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("speed", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("heading", DataTypes.FloatType, true, Metadata.empty())
        });

        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        Dataset modifiedDS = kuduDf.map(new MapFunction<Row, Row>() {
                       @Override
                       public Row call(Row row) throws Exception {
                           return RowFactory.create(row.getLong(0), row.getInt(1), row.getFloat(2), row.getFloat(3), 10.0f, row.getFloat(5));
                       }
                   },
                encoder);

        System.out.println("############### Start Updating ############### ");
        System.out.println(dateFormat.format(new Date()));
        kuduContext.updateRows(modifiedDS, table);
        System.out.println("############### Done Updating ############### ");
        System.out.println(dateFormat.format(new Date()));

        System.out.println("############### Start Second Loading ############### ");
        System.out.println(dateFormat.format(new Date()));
        String registeredTableName2 = "sfmta2";
        sparkSession.sqlContext().read().format("org.apache.kudu.spark.kudu").options(kuduOptions).load()
                .registerTempTable(registeredTableName2);
        Dataset kuduDf2 = sparkSession.sqlContext().sql("SELECT * FROM " + registeredTableName2 + " WHERE speed = 10");
        kuduDf2.show();
        System.out.println("############### Done Second Loading ############### ");
        System.out.println(dateFormat.format(new Date()));
    }

    public static class Sfmta implements Serializable {

        private Long report_time;
        private int vehicle_tag;
        private float longitude;
        private float latitude;
        private float speed;
        private float heading;

        public Sfmta(Long report_time, int vehicle_tag, float longitude, float latitude, float speed, float heading) {
            this.setReport_time(report_time);
            this.setVehicle_tag(vehicle_tag);
            this.setLongitude(longitude);
            this.setLatitude(latitude);
            this.setSpeed(speed);
            this.setHeading(heading);
        }

        public Long getReport_time() {
            return report_time;
        }

        public void setReport_time(Long report_time) {
            this.report_time = report_time;
        }

        public int getVehicle_tag() {
            return vehicle_tag;
        }

        public void setVehicle_tag(int vehicle_tag) {
            this.vehicle_tag = vehicle_tag;
        }

        public float getLongitude() {
            return longitude;
        }

        public void setLongitude(float longitude) {
            this.longitude = longitude;
        }

        public float getLatitude() {
            return latitude;
        }

        public void setLatitude(float latitude) {
            this.latitude = latitude;
        }

        public float getSpeed() {
            return speed;
        }

        public void setSpeed(float speed) {
            this.speed = speed;
        }

        public float getHeading() {
            return heading;
        }

        public void setHeading(float heading) {
            this.heading = heading;
        }
    }
}
