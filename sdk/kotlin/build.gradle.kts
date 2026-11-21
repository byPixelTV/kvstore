import com.google.protobuf.gradle.*
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.gradle.authentication.http.BasicAuthentication

plugins {
    kotlin("jvm") version "2.4.20"
    kotlin("plugin.serialization") version "2.4.20"
    id("com.google.protobuf") version "0.9.5"
    `java-library`
    `maven-publish`
}

group = "dev.bypixel"
version = providers.gradleProperty("sdkVersion").orElse(
    providers.fileContents(layout.projectDirectory.file("../../VERSION")).asText.map { it.trim() + "-SNAPSHOT" },
).get()

repositories { mavenCentral() }

val grpcVersion = "1.78.0"
val grpcKotlinVersion = "1.4.3"
val protobufVersion = "4.33.2"

dependencies {
    api(platform("io.grpc:grpc-bom:$grpcVersion"))
    api("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    api("io.grpc:grpc-protobuf")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    runtimeOnly("io.grpc:grpc-netty-shaded")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    testImplementation(kotlin("test-junit"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }
tasks.withType<Jar>().configureEach {
    from("../../LICENSE") { into("META-INF") }
}
kotlin { compilerOptions.jvmTarget.set(JvmTarget.JVM_17) }

val liveTest = providers.environmentVariable("KVSTORE_SDK_TEST_TARGET").isPresent
tasks.test {
    inputs.property("liveTest", liveTest)
    outputs.upToDateWhen { !liveTest }
}

sourceSets.main {
    proto.srcDir("../../api/proto")
    proto.include("kvstore/v1/kvstore.proto")
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
        id("grpckt") { artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKotlinVersion:jdk8@jar" }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("KVStore Kotlin SDK")
                description.set("Kotlin/JVM gRPC client for KVStore")
                url.set("https://github.com/byPixelTV/kvstore")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/license/mit")
                    }
                }
            }
        }
    }
    repositories {
        maven {
            name = "bypixel"
            val channel = if (version.toString().endsWith("-SNAPSHOT")) "snapshots" else "releases"
            url = uri("https://repo.bypixel.dev/$channel/")
            credentials(PasswordCredentials::class)
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
        maven {
            name = "verification"
            url = layout.buildDirectory.dir("verification-repo").get().asFile.toURI()
        }
    }
}
