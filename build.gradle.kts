plugins {
    `java-library`
    java
    `maven-publish`
    id("me.champeau.mrjar") version "0.1.1"
    idea
    signing
}

group = project.findProperty("projectGroup") as String
version = project.findProperty("projectVersion") as String

multiRelease {
    targetVersions(8, 21)
}

java {
    withSourcesJar()
    withJavadocJar()
}

allprojects {
    repositories {
        mavenCentral()
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = project.findProperty("projectGroup") as String
            artifactId = project.findProperty("projectName") as String
            version = project.findProperty("projectVersion") as String
            from(components["java"])

            pom {
                name.set("Batch4j")
                description.set("Simple batch processing library for java")
                url.set("https://github.com/mawngo/batch4j")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("mawngo")
                        name.set("Nguyen Toan")
                        email.set("sitdownrightnow.dev@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git:github.com/mawngo/batch4j.git")
                    developerConnection.set("scm:git:ssh://github.com/mawngo/batch4j.git")
                    url.set("https://github.com/mawngo/batch4j")
                }
            }
        }
    }
    repositories {
        maven {
            val releasesRepoUrl = layout.buildDirectory.dir("repos/releases")
            val snapshotsRepoUrl = layout.buildDirectory.dir("repos/snapshots")
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl.get() else releasesRepoUrl.get())
        }
    }
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.12")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

tasks.named<Jar>("jar") {
    archiveClassifier.set("")
    isEnabled = true
}

signing {
    sign(publishing.publications["mavenJava"])
}

tasks.test {
    useJUnitPlatform()
}

// Optional: If `java21Test` is a custom test task
tasks.named<Test>("java21Test") {
    useJUnitPlatform()
}
