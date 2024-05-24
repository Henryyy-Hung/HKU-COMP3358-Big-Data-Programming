plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Apache Hadoop Common
    implementation("org.apache.hadoop:hadoop-common:3.3.4")
    // Apache Hadoop MapReduce Client Core
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.1")
    // Apache Spark
    implementation("org.apache.spark:spark-core_2.12:3.1.2")
    implementation("org.apache.spark:spark-sql_2.12:3.1.2")
    // Junit
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}