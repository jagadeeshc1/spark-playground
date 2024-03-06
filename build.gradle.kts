plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.noarg") version "1.9.22"

}


group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

noArg{
    annotation("org.example.NoArgEntity")
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.5.0")
    implementation("org.apache.spark:spark-sql_2.12:3.5.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}