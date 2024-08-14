---
title: Java Setup
rank: 30
---

# Java Setup

::: info
This is only required for [running Spark tests](../spark-tests/).
You do not need Java to build the project.
:::

Please install OpenJDK 17 on your host.
You can use any widely-used OpenJDK distribution, such as [Amazon Corretto](https://aws.amazon.com/corretto/).

It is recommended to set `JAVA_HOME` when following the instructions in the next sections.
If the `JAVA_HOME` environment variable is not set, the Spark build script will try to find the Java installation using
either
(1) the location of `javac` (for Linux), or (2) the output of `/usr/libexec/java_home` (for macOS).
