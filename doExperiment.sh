#!/bin/bash
../apache-maven-3.5.4/bin/mvn exec:java -Dexec.args="192.168.48.219 la < la" 2> test.out &